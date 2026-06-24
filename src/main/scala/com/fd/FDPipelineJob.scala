package com.fd

import org.apache.spark.{SparkConf, SparkContext}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.security.MessageDigest
import java.util.Base64

import scala.collection.mutable
import scala.io.Source

/**
 * End-to-end FD mix pipeline (fd-tools) — MIX_MAPRULE_* env var edition.
 *
 * Reads stdtime.* footprint descriptor files from Linode Object Storage (S3),
 * applies mix_maprule logic (via C++ JNI), and uploads the resulting
 * stdtime.* output file back to S3.
 *
 * Config mode is selected via PER_MAP_PER_CONFIG (default: false):
 *
 *   PER_MAP_PER_CONFIG=true
 *     Generates AddFD configs at runtime from the customer map file.
 *     Requires: CUSTOMER_MAP_S3_KEY — S3 key of the customer map (e.g.
 *       "fd_expansion_configs/fd_compute_customer_map.Key2").
 *     One Spark task is created per network; output files are named
 *     stdtime.<network>.key<N> (e.g. stdtime.v^f.key2).
 *
 *   PER_MAP_PER_CONFIG=false + MIX_MAPRULE_CONFIG set
 *     Downloads config(s) from S3. MIX_MAPRULE_CONFIG may be a single S3
 *     key or a glob pattern (e.g. "fd_config_from_map/N.key2").
 *
 *   PER_MAP_PER_CONFIG=false + MIX_MAPRULE_CONFIG not set
 *     Loads static configs bundled in the JAR under fd_tools_config/.
 *     All files listed in fd_tools_config/MANIFEST are loaded.
 *
 * Common env vars:
 *   MIX_MAPRULE_IDIR               S3 prefix for stdtime.* input files
 *   MIX_MAPRULE_ODIR               S3 prefix for output
 *   MIX_MAPRULE_SUFFIX             Output suffix (optional, S3-config mode only)
 *   MIX_MAPRULE_THINNING_PCT_JUMP  Thinning pct (optional, default 0.0)
 *
 * Credentials / encryption:
 *   S3_ACCESS_KEY             Linode access key
 *   S3_SECRET_KEY             Linode secret key
 *   S3_OUTPUT_KEY_INDEX       Key index used to encrypt uploaded output
 *                               (default: smallest key index available)
 *   S3_ENDPOINT               S3 endpoint (default: us-ord-10.linodeobjects.com)
 *   FD_LOCAL_DEBUG_DIR        If set, intermediate files are also saved here
 *
 * Usage:
 *   spark-submit --class com.fd.FDPipelineJob \
 *     fd-spark_2.13-0.1.jar \
 *     <s3Bucket> <encryptionKeysFile>
 *
 * Arguments:
 *   s3Bucket            Linode bucket name
 *   encryptionKeysFile  Path to JSON file: {"1": "<base64-key>", "2": "..."}
 */
object FDPipelineJob {

  // Linode Object Storage endpoint — overridable via S3_ENDPOINT env var
  def s3Endpoint: String = sys.env.getOrElse("S3_ENDPOINT", "us-ord-10.linodeobjects.com")

  /**
   * Extract the key index from an S3 path based on its .keyN suffix.
   * e.g. "Anirudh/fds/stdtime.mm1.key2" → "2"
   * Defaults to "1" if no .keyN suffix found.
   */
  def keyIndexFromPath(path: String): String = {
    val pattern = """(?i)\.key(\d+)$""".r
    pattern.findFirstMatchIn(path).map(_.group(1)).getOrElse("1")
  }

  /**
   * Strip the .keyN suffix from a filename so C++ tools see the plain name.
   * e.g. "stdtime.mm1.key2" → "stdtime.mm1"
   */
  def stripKeyN(name: String): String = name.replaceAll("""\.key\d+$""", "")

  // -------------------------------------------------------------------------
  // S3 helpers — same pattern as StackDistancePipelineJob in fd-compute-spark
  // -------------------------------------------------------------------------

  /**
   * Load encryption keys from a JSON file.
   * Format: {"1": "<base64-AES-key>", "2": "<base64-AES-key>"}
   * No external JSON library needed — simple regex parse is sufficient.
   */
  def loadEncryptionKeys(path: String): Map[String, String] = {
    val content = Source.fromFile(path).mkString
    val pattern = """"(\w+)"\s*:\s*"([^"]+)"""".r
    pattern.findAllMatchIn(content).map(m => m.group(1) -> m.group(2)).toMap
  }

  /**
   * Build a Hadoop Configuration for S3A with SSE-C encryption.
   * All reads and writes through this config will use the provided AES key.
   */
  def s3aConf(accessKey: String, secretKey: String, sseKeyB64: String): Configuration = {
    val conf = new Configuration(false)
    conf.set("fs.s3a.impl",                               "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("fs.s3a.endpoint",                           s3Endpoint)
    conf.set("fs.s3a.path.style.access",                  "true")
    conf.set("fs.s3a.connection.ssl.enabled",             "true")
    conf.set("fs.s3a.access.key",                         accessKey)
    conf.set("fs.s3a.secret.key",                         secretKey)
    conf.set("fs.s3a.server-side-encryption-algorithm",   "SSE-C")
    conf.set("fs.s3a.server-side-encryption.key",         sseKeyB64)
    val keyBytes = Base64.getDecoder.decode(sseKeyB64)
    val md5B64   = Base64.getEncoder.encodeToString(
                     MessageDigest.getInstance("MD5").digest(keyBytes))
    conf.set("fs.s3a.server-side-encryption-key-md5",     md5B64)
    conf.set("hadoop.tmp.dir", System.getProperty("java.io.tmpdir", "/tmp"))
    conf
  }

  /**
   * List all files under s3a://bucket/prefix.
   * Returns full s3a:// paths. Skips directory placeholder entries.
   */
  def listS3Files(bucket: String, prefix: String, conf: Configuration): Seq[String] = {
    val basePath = new Path(s"s3a://$bucket/$prefix")
    val fs       = FileSystem.newInstance(basePath.toUri, conf)
    val buf      = mutable.ArrayBuffer[String]()
    try {
      val iter = fs.listFiles(basePath, /*recursive=*/ true)
      while (iter.hasNext) {
        val p = iter.next().getPath.toString
        if (!p.endsWith("/")) buf += p
      }
    } finally fs.close()
    buf.toSeq
  }

  /**
   * Download a single S3 file to a local path.
   */
  def downloadS3File(s3Path: String, localPath: java.io.File, conf: Configuration): Unit = {
    val path   = new Path(s3Path)
    val fs     = FileSystem.newInstance(path.toUri, conf)
    val in     = fs.open(path)
    val out    = new java.io.FileOutputStream(localPath)
    try {
      val buf = new Array[Byte](64 * 1024)
      var n   = in.read(buf)
      while (n > 0) { out.write(buf, 0, n); n = in.read(buf) }
    } finally {
      in.close()
      out.close()
      fs.close()
    }
  }

  /**
   * Upload a local file to S3.
   */
  def uploadToS3(localFile: java.io.File, s3Path: String, conf: Configuration): Unit = {
    val path   = new Path(s3Path)
    val fs     = FileSystem.newInstance(path.toUri, conf)
    val out    = fs.create(path, /*overwrite=*/ true)
    val in     = new java.io.FileInputStream(localFile)
    try {
      val buf = new Array[Byte](64 * 1024)
      var n   = in.read(buf)
      while (n > 0) { out.write(buf, 0, n); n = in.read(buf) }
    } finally {
      in.close()
      out.close()
      fs.close()
    }
  }

  // -------------------------------------------------------------------------
  // Main
  // -------------------------------------------------------------------------

  /**
   * fdcompute-spark writes sharded stdtime files with N header lines followed by
   * N copies of each data row (one per shard). The C++ read loop errors on the
   * second # line, silently skipping every file and producing empty output.
   *
   * Fix: rewrite the file keeping only the first # line (the aggregate header)
   * and all non-# lines. The C++ accumulates data rows with +=, so N copies of
   * each row sum correctly to the aggregate totals stated in the first header.
   */
  def normalizeStdtimeHeaders(f: java.io.File, logPrefix: String): Unit = {
    val lines  = scala.io.Source.fromFile(f).getLines().toList
    val hdrIdx = lines.lastIndexWhere(_.startsWith("#"))
    if (hdrIdx <= 0) return  // 0 or 1 header — nothing to fix
    val header   = lines.head
    val dataLines = lines.filterNot(_.startsWith("#"))
    val out = new java.io.PrintWriter(f)
    try { (header +: dataLines).foreach(out.println) } finally out.close()
    println(s"[$logPrefix]   Normalized ${f.getName}: stripped $hdrIdx extra header line(s)")
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        "Usage: FDPipelineJob <s3Bucket> <encryptionKeysFile>\n" +
        "\n" +
        "Config mode (PER_MAP_PER_CONFIG, default false):\n" +
        "  true   Generate configs at runtime from CUSTOMER_MAP_S3_KEY\n" +
        "  false  MIX_MAPRULE_CONFIG set → S3 download\n" +
        "         MIX_MAPRULE_CONFIG unset → bundled JAR configs\n" +
        "\n" +
        "Env vars:\n" +
        "  MIX_MAPRULE_IDIR               S3 prefix for stdtime.* input files\n" +
        "  MIX_MAPRULE_ODIR               S3 prefix for output\n" +
        "  PER_MAP_PER_CONFIG             true|false (default false)\n" +
        "  CUSTOMER_MAP_S3_KEY            Required when PER_MAP_PER_CONFIG=true\n" +
        "  MIX_MAPRULE_CONFIG             S3 config key or glob (PER_MAP_PER_CONFIG=false)\n" +
        "  MIX_MAPRULE_SUFFIX             Output suffix override (S3-config mode only)\n" +
        "  MIX_MAPRULE_THINNING_PCT_JUMP  Thinning pct (optional, default 0.0)\n" +
        "  S3_ACCESS_KEY, S3_SECRET_KEY, S3_OUTPUT_KEY_INDEX, S3_ENDPOINT\n"
      )
      System.exit(1)
    }

    val s3Bucket           = args(0)
    val encryptionKeysFile = args(1)

    val conf = new SparkConf().setAppName("FDPipelineJob")
    if (!conf.contains("spark.master")) conf.setMaster("local[*]")

    val sc = new SparkContext(conf)
    try {
      run(sc, s3Bucket, encryptionKeysFile)
      println("Mix pipeline complete.")
    } finally {
      sc.stop()
    }
  }

  // -------------------------------------------------------------------------
  // Pipeline
  // -------------------------------------------------------------------------

  def run(
    sc:                 SparkContext,
    s3Bucket:           String,
    encryptionKeysFile: String
  ): Unit = {

    val accessKey = sys.env.getOrElse("S3_ACCESS_KEY",
      throw new IllegalStateException("S3_ACCESS_KEY env var not set"))
    val secretKey = sys.env.getOrElse("S3_SECRET_KEY",
      throw new IllegalStateException("S3_SECRET_KEY env var not set"))

    // ------------------------------------------------------------------
    // Read required env vars
    // ------------------------------------------------------------------
    val mixIdirRaw = sys.env.getOrElse("MIX_MAPRULE_IDIR",
      throw new IllegalStateException("MIX_MAPRULE_IDIR env var not set"))
    val mixOdirRaw = sys.env.getOrElse("MIX_MAPRULE_ODIR",
      throw new IllegalStateException("MIX_MAPRULE_ODIR env var not set"))

    val perMapPerConfig = sys.env.get("PER_MAP_PER_CONFIG")
      .exists(v => v.equalsIgnoreCase("true") || v == "1")
    val mixConfigKeyOpt = sys.env.get("MIX_MAPRULE_CONFIG").filter(_.nonEmpty)

    val thinningPct = sys.env.get("MIX_MAPRULE_THINNING_PCT_JUMP")
      .filter(_.nonEmpty).map(_.toDouble).getOrElse(0.0)

    // Strip any leading "s3://<bucket>/" or "s3a://<bucket>/" to get bare S3 keys
    val stripBucketPrefix = { raw: String =>
      val s3Pattern = s"^s3a?://${java.util.regex.Pattern.quote(s3Bucket)}/".r
      s3Pattern.replaceFirstIn(raw, "")
    }
    val mixIdir = stripBucketPrefix(mixIdirRaw).stripSuffix("/")
    val mixOdir = stripBucketPrefix(mixOdirRaw).stripSuffix("/")

    // ------------------------------------------------------------------
    // Load encryption keys
    // ------------------------------------------------------------------
    val encryptionKeys = loadEncryptionKeys(encryptionKeysFile)
    println(s"[DEBUG] Loaded ${encryptionKeys.size} encryption key(s) from $encryptionKeysFile")
    encryptionKeys.keys.toSeq.sorted.foreach(k => println(s"[DEBUG]   key index $k loaded"))

    // A conf that can list S3 (listing doesn't need SSE-C key content, only credentials)
    val listConf = s3aConf(accessKey, secretKey, encryptionKeys(encryptionKeys.keys.min))

    // ------------------------------------------------------------------
    // Discover stdtime.* input files in S3 under MIX_MAPRULE_IDIR
    // ------------------------------------------------------------------
    println(s"[DEBUG] Scanning S3 for stdtime files under s3://$s3Bucket/$mixIdir ...")
    val s3InputFiles = listS3Files(s3Bucket, mixIdir, listConf)
      .filter(p => p.split("/").last.startsWith("stdtime"))
    println(s"[DEBUG] Found ${s3InputFiles.size} stdtime file(s):")
    s3InputFiles.foreach { f =>
      val keyIdx    = keyIndexFromPath(f)
      val localName = stripKeyN(f.split("/").last)
      println(s"[DEBUG]   $f  →  local name: $localName  (key index: $keyIdx)")
    }

    // ------------------------------------------------------------------
    // Resolve config items — (suffix, configLines) per Spark task.
    //
    // Mode 1 PER_MAP_PER_CONFIG=true:
    //   Download customer map from CUSTOMER_MAP_S3_KEY, generate one config
    //   per network in-memory. Output files are named stdtime.<network>.
    //
    // Mode 2 MIX_MAPRULE_CONFIG set (PER_MAP_PER_CONFIG=false):
    //   Download config(s) from S3. Supports single key or glob pattern.
    //
    // Mode 3 neither set:
    //   Load static configs bundled in the JAR under fd_tools_config/.
    // ------------------------------------------------------------------

    def globToRegex(glob: String): java.util.regex.Pattern = {
      val sb = new StringBuilder("^")
      glob.foreach {
        case '*' => sb.append("[^/]*")
        case '.' => sb.append("\\.")
        case c   => sb.append(java.util.regex.Pattern.quote(c.toString))
      }
      sb.append("$")
      java.util.regex.Pattern.compile(sb.toString)
    }

    val configItems: Seq[(String, String)] = if (perMapPerConfig) {

      // ── Mode 1: customer map ──────────────────────────────────────────
      val customerMapKey = sys.env.getOrElse("CUSTOMER_MAP_S3_KEY",
        throw new IllegalStateException(
          "CUSTOMER_MAP_S3_KEY must be explicitly set when PER_MAP_PER_CONFIG=true"))
      val bareKey = stripBucketPrefix(customerMapKey)
      val keyIdx  = keyIndexFromPath(bareKey.split("/").last)

      println(s"[DEBUG] Mode: PER_MAP_PER_CONFIG — customer map s3://$s3Bucket/$bareKey (key $keyIdx)")
      val tmp = java.io.File.createTempFile("fd_customer_map_", ".tmp")
      val mapContent = try {
        downloadS3File(s"s3a://$s3Bucket/$bareKey", tmp,
          s3aConf(accessKey, secretKey, encryptionKeys(keyIdx)))
        scala.io.Source.fromFile(tmp).mkString
      } finally tmp.delete()

      // Parse customer map: group VCD extensions by network (alphabetical)
      val networkToVcds = scala.collection.mutable.TreeMap[String, scala.collection.mutable.ArrayBuffer[String]]()
      for (line <- mapContent.split("\\n").iterator) {
        val trimmed = line.trim
        if (trimmed.nonEmpty) {
          val parts = trimmed.split("\\s+")
          if (parts.length >= 4) {
            val ext       = parts(3)
            val lastCaret = ext.lastIndexOf('^')
            if (lastCaret >= 0) {
              val network = ext.substring(0, lastCaret)
              networkToVcds.getOrElseUpdate(network, scala.collection.mutable.ArrayBuffer()) += ext
            }
          }
        }
      }

      val items = networkToVcds.map { case (network, vcds) =>
        val sortedLines = vcds.sorted.map(_ + " add 1").mkString("", "\n", "\n")
        println(s"[DEBUG]   network=$network  vcds=${vcds.size}")
        (network, sortedLines)
      }.toSeq

      println(s"[DEBUG] Generated ${items.size} configs from customer map")
      items

    } else if (mixConfigKeyOpt.isDefined) {

      // ── Mode 2: S3 config (single key or glob) ────────────────────────
      val mixCfgKey = stripBucketPrefix(mixConfigKeyOpt.get)
      println(s"[DEBUG] Mode: S3 config — MIX_MAPRULE_CONFIG=s3://$s3Bucket/$mixCfgKey")

      if (mixCfgKey.contains("*")) {
        val prefixUpToStar = mixCfgKey.substring(0, mixCfgKey.indexOf('*'))
        val listPrefix     = prefixUpToStar.substring(0, prefixUpToStar.lastIndexOf('/') + 1)
        val regex          = globToRegex(mixCfgKey)
        println(s"[DEBUG] Glob pattern — scanning s3://$s3Bucket/$listPrefix")
        val matched = listS3Files(s3Bucket, listPrefix, listConf)
          .map(_.stripPrefix(s"s3a://$s3Bucket/"))
          .filter(k => regex.matcher(k).matches())
          .sorted
        println(s"[DEBUG] Matched ${matched.size} config file(s):")
        matched.foreach(k => println(s"[DEBUG]   $k"))
        matched.map { key =>
          val fileName  = key.split("/").last
          val cfgKeyIdx = keyIndexFromPath(fileName)
          val cfgSuffix = stripKeyN(fileName)
          val tmp       = java.io.File.createTempFile("fd_cfg_", ".tmp")
          val lines = try {
            downloadS3File(s"s3a://$s3Bucket/$key", tmp,
              s3aConf(accessKey, secretKey, encryptionKeys(cfgKeyIdx)))
            scala.io.Source.fromFile(tmp).mkString
          } finally tmp.delete()
          println(s"[DEBUG]   → suffix='$cfgSuffix'  (${lines.split("\\n").length} lines)")
          (cfgSuffix, lines)
        }
      } else {
        val cfgFileName  = mixCfgKey.split("/").last
        val cfgKeyIdx    = keyIndexFromPath(cfgFileName)
        val cfgLocalName = stripKeyN(cfgFileName)
        val cfgSuffix    = sys.env.get("MIX_MAPRULE_SUFFIX").filter(_.nonEmpty)
          .getOrElse(cfgLocalName.stripSuffix(".config"))
        println(s"[DEBUG] Single config — key index $cfgKeyIdx  suffix='$cfgSuffix'")
        val tmp = java.io.File.createTempFile("fd_config_", ".tmp")
        val lines: String = try {
          downloadS3File(s"s3a://$s3Bucket/$mixCfgKey", tmp,
            s3aConf(accessKey, secretKey, encryptionKeys(cfgKeyIdx)))
          val l = scala.io.Source.fromFile(tmp).mkString
          println(s"[DEBUG] Config contents (${l.split("\\n").length} lines):")
          l.split("\n").foreach(ln => println(s"[DEBUG]   $ln"))
          l
        } finally tmp.delete()
        Seq((cfgSuffix, lines))
      }

    } else {

      // ── Mode 3: bundled JAR configs ───────────────────────────────────
      println(s"[DEBUG] Mode: bundled JAR configs (fd_tools_config/)")
      val manifestStream = getClass.getResourceAsStream("/fd_tools_config/MANIFEST")
      if (manifestStream == null)
        throw new IllegalStateException(
          "fd_tools_config/MANIFEST not found in JAR — rebuild with src/main/resources/fd_tools_config/ in place")
      val manifest = scala.io.Source.fromInputStream(manifestStream)
        .getLines().map(_.trim).filter(f => f.nonEmpty && f != "MANIFEST").toSeq
      println(s"[DEBUG] Manifest: ${manifest.size} config file(s)")

      manifest.flatMap { fname =>
        val stream = getClass.getResourceAsStream(s"/fd_tools_config/$fname")
        if (stream == null) {
          println(s"[DEBUG] WARNING: $fname in manifest but missing from JAR — skipping")
          None
        } else {
          val content = scala.io.Source.fromInputStream(stream).mkString
          println(s"[DEBUG]   $fname  (${content.split("\\n").length} lines)")
          Some((fname, content))
        }
      }
    }

    println(s"[DEBUG] Configs to process  : ${configItems.size}")
    println(s"[DEBUG] MIX_MAPRULE_IDIR   : s3://$s3Bucket/$mixIdir")
    println(s"[DEBUG] MIX_MAPRULE_ODIR   : s3://$s3Bucket/$mixOdir")
    println(s"[DEBUG] Thinning pct       : $thinningPct")

    // ------------------------------------------------------------------
    // Key index for output encryption
    // ------------------------------------------------------------------
    val outKeyIdx = sys.env.get("S3_OUTPUT_KEY_INDEX").filter(_.nonEmpty)
      .getOrElse(encryptionKeys.keys.toSeq.sorted.last)
    println(s"[DEBUG] Output encryption key index: $outKeyIdx  (S3_OUTPUT_KEY_INDEX)")

    // ------------------------------------------------------------------
    // Optional local debug dir for intermediate files
    // ------------------------------------------------------------------
    val localDebugDir: Option[java.io.File] = sys.env.get("FD_LOCAL_DEBUG_DIR")
      .filter(_.nonEmpty)
      .map { p => val d = new java.io.File(p); d.mkdirs(); d }
    localDebugDir match {
      case Some(d) => println(s"[DEBUG] Intermediate files will be saved to: ${d.getAbsolutePath}")
      case None    => println(s"[DEBUG] FD_LOCAL_DEBUG_DIR not set — intermediate files will not be saved locally")
    }

    // ------------------------------------------------------------------
    // Broadcast everything to executors
    // ------------------------------------------------------------------
    val bcAccessKey      = sc.broadcast(accessKey)
    val bcSecretKey      = sc.broadcast(secretKey)
    val bcEncryptionKeys = sc.broadcast(encryptionKeys)
    val bcS3InputFiles   = sc.broadcast(s3InputFiles)
    val bcS3Bucket       = sc.broadcast(s3Bucket)
    val bcMixOdir        = sc.broadcast(mixOdir)
    val bcOutKeyIdx      = sc.broadcast(outKeyIdx)
    val bcLocalDebugDir  = sc.broadcast(localDebugDir.map(_.getAbsolutePath))
    val bcThinningPct    = sc.broadcast(thinningPct)

    // ------------------------------------------------------------------
    // MAP phase: one Spark task per config file.
    //   1. Download all stdtime.* input files from S3
    //   2. Run C++ mix (FDNative.runMix)
    //   3. (optional) Copy to FD_LOCAL_DEBUG_DIR
    //   4. Upload output to MIX_MAPRULE_ODIR encrypted with S3_OUTPUT_KEY_INDEX
    //   5. Clean up temp dirs
    // configItems has one entry per resolved config (1 for single, N for glob).
    // ------------------------------------------------------------------
    val results: Array[(String, Int)] = sc.parallelize(configItems, configItems.size.max(1))
      .map { case (suffix, configLines) =>

        val ak            = bcAccessKey.value
        val sk            = bcSecretKey.value
        val keys          = bcEncryptionKeys.value
        val inputFiles    = bcS3InputFiles.value
        val bucket        = bcS3Bucket.value
        val outputPfx     = bcMixOdir.value
        val outKeyIdx     = bcOutKeyIdx.value
        val debugDirPath  = bcLocalDebugDir.value
        val thinningPct   = bcThinningPct.value

        val taskId    = java.util.UUID.randomUUID().toString
        val localBase = new java.io.File(s"/tmp/fd_mix_$taskId")
        val inputDir  = new java.io.File(localBase, "input")
        val outputDir = new java.io.File(localBase, "output")
        inputDir.mkdirs()
        outputDir.mkdirs()

        println(s"[$suffix] ── Starting mix task ──────────────────────────")
        println(s"[$suffix] Task id          : $taskId")
        println(s"[$suffix] Input files      : ${inputFiles.size}")
        println(s"[$suffix] Local input dir  : ${inputDir.getAbsolutePath}")
        println(s"[$suffix] Local output dir : ${outputDir.getAbsolutePath}")
        println(s"[$suffix] Thinning pct     : $thinningPct")
        println(s"[$suffix] Config lines:")
        configLines.split("\n").foreach(l => println(s"[$suffix]   $l"))

        try {
          // -- Stage 1: Download stdtime.* files using correct key per file --
          println(s"[$suffix] Stage 1: Downloading ${inputFiles.size} input file(s) from S3...")
          for (s3Path <- inputFiles) {
            val rawName   = s3Path.split("/").last
            val keyIdx    = keyIndexFromPath(rawName)
            val localName = stripKeyN(rawName)
            val localFile = new java.io.File(inputDir, localName)
            println(s"[$suffix]   $rawName  →  ${localFile.getAbsolutePath}  (key index: $keyIdx)")
            val dlConf = s3aConf(ak, sk, keys(keyIdx))
            downloadS3File(s3Path, localFile, dlConf)
            println(s"[$suffix]   Downloaded ${localFile.length()} bytes")
            // fdcompute writes multi-shard files: N header lines then N repetitions of each
            // data row. The C++ reader only handles one # header; strip extras so it works.
            normalizeStdtimeHeaders(localFile, suffix)
          }
          println(s"[$suffix] Stage 1: Done. Local input dir contents:")
          Option(inputDir.listFiles()).getOrElse(Array.empty).foreach { f =>
            println(s"[$suffix]   ${f.getName}  (${f.length()} bytes)")
          }

          // -- Stage 2: Run C++ mix --
          println(s"[$suffix] Stage 2: Running FDNative.runMix(suffix=$suffix, thinningPct=$thinningPct)...")
          val ret = FDNative.runMix(
            configLines = configLines,
            inputDir    = inputDir.getAbsolutePath,
            outputDir   = outputDir.getAbsolutePath,
            suffix      = suffix,
            thinningPct = thinningPct
          )
          println(s"[$suffix] Stage 2: runMix returned: $ret")

          if (ret != 0) {
            System.err.println(s"[$suffix] ERROR: runMix returned non-zero exit code $ret")
            (suffix, ret)
          } else {
            // -- Stage 3: Show output files --
            val outFiles = Option(outputDir.listFiles())
              .getOrElse(Array.empty)
              .filter(_.isFile)

            println(s"[$suffix] Stage 3: Output dir contains ${outFiles.length} file(s):")
            outFiles.foreach { f =>
              println(s"[$suffix]   ${f.getName}  (${f.length()} bytes)")
              val lines = scala.io.Source.fromFile(f).getLines().take(10).toSeq
              println(s"[$suffix]   --- first ${lines.size} lines ---")
              lines.foreach(l => println(s"[$suffix]   $l"))
              println(s"[$suffix]   ---")
            }

            // -- Stage 3b: Save intermediate files locally for inspection --
            debugDirPath.foreach { dirPath =>
              val debugBase = new java.io.File(s"$dirPath/$suffix")
              val debugIn   = new java.io.File(debugBase, "input")
              val debugOut  = new java.io.File(debugBase, "output")
              debugIn.mkdirs()
              debugOut.mkdirs()

              Option(inputDir.listFiles()).getOrElse(Array.empty).foreach { src =>
                val dst = new java.io.File(debugIn, src.getName)
                java.nio.file.Files.copy(src.toPath, dst.toPath,
                  java.nio.file.StandardCopyOption.REPLACE_EXISTING)
              }
              outFiles.foreach { src =>
                val dst = new java.io.File(debugOut, src.getName)
                java.nio.file.Files.copy(src.toPath, dst.toPath,
                  java.nio.file.StandardCopyOption.REPLACE_EXISTING)
              }
              println(s"[$suffix] Stage 3b: Intermediate files saved to $debugBase")
              println(s"[$suffix]   input/  : ${Option(debugIn.listFiles()).map(_.length).getOrElse(0)} file(s)")
              println(s"[$suffix]   output/ : ${outFiles.length} file(s)")
            }

            // -- Stage 4: Upload output files to MIX_MAPRULE_ODIR --
            println(s"[$suffix] Stage 4: Uploading ${outFiles.length} file(s) to s3://$bucket/$outputPfx/ (key index: $outKeyIdx)...")
            val upConf = s3aConf(ak, sk, keys(outKeyIdx))
            for (outFile <- outFiles) {
              val s3Name = s"${outFile.getName}.key$outKeyIdx"
              val dest   = s"s3a://$bucket/$outputPfx/$s3Name"
              println(s"[$suffix]   Uploading ${outFile.getName} → $dest  (SSE-C key $outKeyIdx)")
              uploadToS3(outFile, dest, upConf)
              println(s"[$suffix]   Upload complete: s3://$bucket/$outputPfx/$s3Name")
            }
            println(s"[$suffix] Done ✓  Uploaded ${outFiles.length} file(s) to s3://$bucket/$outputPfx/")
            (suffix, 0)
          }
        } finally {
          def deleteRecursively(f: java.io.File): Unit = {
            if (f.isDirectory) f.listFiles().foreach(deleteRecursively)
            f.delete()
          }
          deleteRecursively(localBase)
          println(s"[$suffix] Cleaned up temp dir ${localBase.getAbsolutePath}")
        }
      }
      .collect()

    val ok     = results.count(_._2 == 0)
    val failed = results.filter(_._2 != 0)
    println(s"\n=== Mix pipeline: $ok succeeded, ${failed.length} failed ===")
    if (failed.nonEmpty) {
      println("Failed configs:")
      failed.foreach { case (suffix, code) => println(s"  $suffix (exit code $code)") }
    }
  }
}
