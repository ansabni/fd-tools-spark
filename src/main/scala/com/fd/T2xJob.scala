package com.fd

import org.apache.spark.{SparkConf, SparkContext}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.security.MessageDigest
import java.util.Base64

import scala.io.Source

/**
 * Shared end-to-end pipeline used by both T2sJob and T2cJob.
 *
 * Downloads one stdtime file from S3, runs the C++ t2s or t2c logic via JNI,
 * and uploads the resulting stdspace / stdcount file back to S3.
 *
 * The two thin job objects (T2sJob, T2cJob) call run() with their own mode.
 *
 * Configuration via environment variables (mirrors the standalone binaries):
 *
 *   T2S_IDIR / T2C_IDIR  S3 path to the input stdtime file   (required)
 *   T2S_ODIR / T2C_ODIR  S3 path for the output file          (required)
 *
 * Both IDIR and ODIR are FILE paths (not directories), matching the
 * inherited Hadoop env-var convention used by `unshard`.
 *
 * Credentials / encryption (same as FDPipelineJob):
 *   S3_ACCESS_KEY, S3_SECRET_KEY, S3_ENDPOINT
 *   S3_OUTPUT_KEY_INDEX  Key index used to encrypt the uploaded output
 *                        (default: smallest key index available)
 *   FD_LOCAL_DEBUG_DIR   If set, intermediate files are also saved here
 */
object T2xJob {

  /** Pipeline mode — selects which JNI call to invoke and which env vars to read. */
  sealed trait Mode {
    def name:     String                                         // "t2s" / "t2c"
    def idirEnv:  String                                         // "T2S_IDIR" / "T2C_IDIR"
    def odirEnv:  String                                         // "T2S_ODIR" / "T2C_ODIR"
    def runJni(in: String, out: String): Int                     // FDNative.runT2s / runT2c
  }
  case object T2sMode extends Mode {
    val name    = "t2s"
    val idirEnv = "T2S_IDIR"
    val odirEnv = "T2S_ODIR"
    def runJni(in: String, out: String): Int = FDNative.runT2s(in, out)
  }
  case object T2cMode extends Mode {
    val name    = "t2c"
    val idirEnv = "T2C_IDIR"
    val odirEnv = "T2C_ODIR"
    def runJni(in: String, out: String): Int = FDNative.runT2c(in, out)
  }

  // -------------------------------------------------------------------------
  // S3 / encryption helpers — copy of UnshardJob's helpers.
  // -------------------------------------------------------------------------

  def s3Endpoint: String = sys.env.getOrElse("S3_ENDPOINT", "us-ord-10.linodeobjects.com")

  def keyIndexFromPath(path: String): String = {
    val pattern = """\.key(\d+)$""".r
    pattern.findFirstMatchIn(path).map(_.group(1)).getOrElse("1")
  }

  def stripKeyN(name: String): String = name.replaceAll("""\.key\d+$""", "")

  def loadEncryptionKeys(path: String): Map[String, String] = {
    val content = Source.fromFile(path).mkString
    val pattern = """"(\w+)"\s*:\s*"([^"]+)"""".r
    pattern.findAllMatchIn(content).map(m => m.group(1) -> m.group(2)).toMap
  }

  def s3aConf(accessKey: String, secretKey: String, sseKeyB64: String): Configuration = {
    val conf = new Configuration(false)
    conf.set("fs.s3a.impl",                             "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("fs.s3a.endpoint",                         s3Endpoint)
    conf.set("fs.s3a.path.style.access",                "true")
    conf.set("fs.s3a.connection.ssl.enabled",           "true")
    conf.set("fs.s3a.access.key",                       accessKey)
    conf.set("fs.s3a.secret.key",                       secretKey)
    conf.set("fs.s3a.server-side-encryption-algorithm", "SSE-C")
    conf.set("fs.s3a.server-side-encryption.key",       sseKeyB64)
    val keyBytes = Base64.getDecoder.decode(sseKeyB64)
    val md5B64   = Base64.getEncoder.encodeToString(
                     MessageDigest.getInstance("MD5").digest(keyBytes))
    conf.set("fs.s3a.server-side-encryption-key-md5",   md5B64)
    conf.set("hadoop.tmp.dir", System.getProperty("java.io.tmpdir", "/tmp"))
    conf
  }

  def downloadS3File(s3Path: String, localPath: java.io.File, conf: Configuration): Unit = {
    val path = new Path(s3Path)
    val fs   = FileSystem.get(path.toUri, conf)
    val in   = fs.open(path)
    val out  = new java.io.FileOutputStream(localPath)
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

  def uploadToS3(localFile: java.io.File, s3Path: String, conf: Configuration): Unit = {
    val path = new Path(s3Path)
    val fs   = FileSystem.get(path.toUri, conf)
    val out  = fs.create(path, /*overwrite=*/ true)
    val in   = new java.io.FileInputStream(localFile)
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
  // Entry point shared by T2sJob and T2cJob
  // -------------------------------------------------------------------------

  def main(mode: Mode, args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        s"Usage: ${mode.name}Job <s3Bucket> <encryptionKeysFile>\n" +
        "\n" +
        "Configuration via env vars:\n" +
        s"  ${mode.idirEnv}    S3 path to input stdtime file   (required)\n" +
        s"  ${mode.odirEnv}    S3 path for output file         (required)\n" +
        "\n" +
        "Credentials:\n" +
        "  S3_ACCESS_KEY, S3_SECRET_KEY, S3_OUTPUT_KEY_INDEX, S3_ENDPOINT\n"
      )
      System.exit(1)
    }

    val s3Bucket           = args(0)
    val encryptionKeysFile = args(1)

    val conf = new SparkConf().setAppName(s"${mode.name.capitalize}Job")
    if (!conf.contains("spark.master")) conf.setMaster("local[*]")

    val sc = new SparkContext(conf)
    try {
      run(sc, s3Bucket, encryptionKeysFile, mode)
      println(s"${mode.name} pipeline complete.")
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
    encryptionKeysFile: String,
    mode:               Mode
  ): Unit = {

    val accessKey = sys.env.getOrElse("S3_ACCESS_KEY",
      throw new IllegalStateException("S3_ACCESS_KEY env var not set"))
    val secretKey = sys.env.getOrElse("S3_SECRET_KEY",
      throw new IllegalStateException("S3_SECRET_KEY env var not set"))

    // ------------------------------------------------------------------
    // Read mode-specific *_IDIR / *_ODIR env vars
    // ------------------------------------------------------------------
    val idirRaw = sys.env.getOrElse(mode.idirEnv,
      throw new IllegalStateException(s"${mode.idirEnv} env var not set"))
    val odirRaw = sys.env.getOrElse(mode.odirEnv,
      throw new IllegalStateException(s"${mode.odirEnv} env var not set"))

    // Strip any leading "s3://<bucket>/" or "s3a://<bucket>/" to get bare keys
    val stripBucketPrefix = { raw: String =>
      val s3Pattern = s"^s3a?://${java.util.regex.Pattern.quote(s3Bucket)}/".r
      s3Pattern.replaceFirstIn(raw, "").stripPrefix("/")
    }
    val inputKey  = stripBucketPrefix(idirRaw)
    val outputKey = stripBucketPrefix(odirRaw)

    // ------------------------------------------------------------------
    // Load encryption keys
    // ------------------------------------------------------------------
    val encryptionKeys = loadEncryptionKeys(encryptionKeysFile)
    println(s"[DEBUG] Loaded ${encryptionKeys.size} encryption key(s) from $encryptionKeysFile")
    encryptionKeys.keys.toSeq.sorted.foreach(k => println(s"[DEBUG]   key index $k loaded"))

    val inputKeyIdx = keyIndexFromPath(inputKey)
    val outKeyIdx   = sys.env.get("S3_OUTPUT_KEY_INDEX").filter(_.nonEmpty)
      .getOrElse(encryptionKeys.keys.toSeq.sorted.last)

    println(s"[DEBUG] ${mode.idirEnv}    : s3://$s3Bucket/$inputKey  (input key index: $inputKeyIdx)")
    println(s"[DEBUG] ${mode.odirEnv}    : s3://$s3Bucket/$outputKey")
    println(s"[DEBUG] Output enc key  : $outKeyIdx  (S3_OUTPUT_KEY_INDEX)")

    // ------------------------------------------------------------------
    // Optional local debug dir
    // ------------------------------------------------------------------
    val localDebugDir: Option[java.io.File] = sys.env.get("FD_LOCAL_DEBUG_DIR")
      .filter(_.nonEmpty)
      .map { p => val d = new java.io.File(p); d.mkdirs(); d }
    localDebugDir match {
      case Some(d) => println(s"[DEBUG] Intermediate files will be saved to: ${d.getAbsolutePath}")
      case None    => println(s"[DEBUG] FD_LOCAL_DEBUG_DIR not set — intermediates will not be saved locally")
    }

    // ------------------------------------------------------------------
    // Broadcast everything to executors
    // ------------------------------------------------------------------
    val bcAccessKey      = sc.broadcast(accessKey)
    val bcSecretKey      = sc.broadcast(secretKey)
    val bcEncryptionKeys = sc.broadcast(encryptionKeys)
    val bcS3Bucket       = sc.broadcast(s3Bucket)
    val bcInputKey       = sc.broadcast(inputKey)
    val bcOutputKey      = sc.broadcast(outputKey)
    val bcInputKeyIdx    = sc.broadcast(inputKeyIdx)
    val bcOutKeyIdx      = sc.broadcast(outKeyIdx)
    val bcLocalDebugDir  = sc.broadcast(localDebugDir.map(_.getAbsolutePath))
    val bcModeName       = sc.broadcast(mode.name)
    val bcIsT2s          = sc.broadcast(mode == T2sMode)

    // ------------------------------------------------------------------
    // MAP phase: single Spark task
    //   1. Download input file from S3
    //   2. Run C++ t2s / t2c via JNI
    //   3. (optional) Copy to FD_LOCAL_DEBUG_DIR
    //   4. Upload output file to S3
    //   5. Clean up temp dir
    // ------------------------------------------------------------------
    val results: Array[Int] = sc.parallelize(Seq(0), 1)
      .map { _ =>
        val ak           = bcAccessKey.value
        val sk           = bcSecretKey.value
        val keys         = bcEncryptionKeys.value
        val bucket       = bcS3Bucket.value
        val inKey        = bcInputKey.value
        val outKey       = bcOutputKey.value
        val inIdx        = bcInputKeyIdx.value
        val outIdx       = bcOutKeyIdx.value
        val debugDirPath = bcLocalDebugDir.value
        val modeName     = bcModeName.value
        val isT2s        = bcIsT2s.value

        val taskId    = java.util.UUID.randomUUID().toString
        val localBase = new java.io.File(s"/tmp/fd_${modeName}_$taskId")
        localBase.mkdirs()

        // Local file names — strip .keyN if present
        val inFileName  = stripKeyN(inKey.split("/").last)
        val outFileName = stripKeyN(outKey.split("/").last)
        val localIn     = new java.io.File(localBase, s"in_$inFileName")
        val localOut    = new java.io.File(localBase, s"out_$outFileName")

        println(s"[$modeName] ── Starting $modeName task ──────────────────")
        println(s"[$modeName] Task id          : $taskId")
        println(s"[$modeName] Local input file : ${localIn.getAbsolutePath}")
        println(s"[$modeName] Local output file: ${localOut.getAbsolutePath}")

        try {
          // -- Stage 1: Download input from S3 --
          println(s"[$modeName] Stage 1: Downloading s3://$bucket/$inKey  (key index: $inIdx)")
          val dlConf = s3aConf(ak, sk, keys(inIdx))
          downloadS3File(s"s3a://$bucket/$inKey", localIn, dlConf)
          println(s"[$modeName]   Downloaded ${localIn.length()} bytes")

          // -- Stage 2: Run C++ t2s / t2c --
          println(s"[$modeName] Stage 2: Running FDNative.run${modeName.capitalize}...")
          val ret =
            if (isT2s) FDNative.runT2s(localIn.getAbsolutePath, localOut.getAbsolutePath)
            else       FDNative.runT2c(localIn.getAbsolutePath, localOut.getAbsolutePath)
          println(s"[$modeName] Stage 2: run${modeName.capitalize} returned: $ret")

          if (ret != 0) {
            System.err.println(s"[$modeName] ERROR: run${modeName.capitalize} returned non-zero exit code $ret")
            ret
          } else if (!localOut.exists()) {
            System.err.println(s"[$modeName] ERROR: output file ${localOut.getAbsolutePath} not produced")
            1
          } else {
            println(s"[$modeName] Stage 3: Output file ${localOut.getName} (${localOut.length()} bytes)")
            val preview = scala.io.Source.fromFile(localOut).getLines().take(5).toSeq
            println(s"[$modeName]   --- first ${preview.size} lines ---")
            preview.foreach(l => println(s"[$modeName]   $l"))
            println(s"[$modeName]   ---")

            // -- Stage 3b: Save intermediate files locally for inspection --
            debugDirPath.foreach { dirPath =>
              val debugBase = new java.io.File(s"$dirPath/$modeName")
              debugBase.mkdirs()
              val dstIn  = new java.io.File(debugBase, s"input_$inFileName")
              val dstOut = new java.io.File(debugBase, s"output_$outFileName")
              java.nio.file.Files.copy(localIn.toPath, dstIn.toPath,
                java.nio.file.StandardCopyOption.REPLACE_EXISTING)
              java.nio.file.Files.copy(localOut.toPath, dstOut.toPath,
                java.nio.file.StandardCopyOption.REPLACE_EXISTING)
              println(s"[$modeName] Stage 3b: Intermediate files saved to ${debugBase.getAbsolutePath}")
            }

            // -- Stage 4: Upload to S3 --
            // Append .key<idx> if the output key doesn't already have one
            val s3OutKey =
              if ("""\.key\d+$""".r.findFirstIn(outKey).isDefined) outKey
              else s"$outKey.key$outIdx"
            val dest = s"s3a://$bucket/$s3OutKey"
            println(s"[$modeName] Stage 4: Uploading to $dest  (SSE-C key $outIdx)")
            val upConf = s3aConf(ak, sk, keys(outIdx))
            uploadToS3(localOut, dest, upConf)
            println(s"[$modeName]   Upload complete: s3://$bucket/$s3OutKey")
            println(s"[$modeName] Done ✓")
            0
          }
        } finally {
          def deleteRecursively(f: java.io.File): Unit = {
            if (f.isDirectory) f.listFiles().foreach(deleteRecursively)
            f.delete()
          }
          deleteRecursively(localBase)
          println(s"[$modeName] Cleaned up temp dir ${localBase.getAbsolutePath}")
        }
      }
      .collect()

    val ok     = results.count(_ == 0)
    val failed = results.length - ok
    println(s"\n=== ${mode.name} pipeline: $ok succeeded, $failed failed ===")
    if (failed > 0) {
      throw new RuntimeException(s"${mode.name} pipeline failed (exit codes: ${results.mkString(",")})")
    }
  }
}

/**
 * Spark job — stdtime → stdspace (equivalent to the t2s binary).
 *
 *   T2S_IDIR   S3 path to input stdtime file (required)
 *   T2S_ODIR   S3 path for output stdspace file (required)
 */
object T2sJob {
  def main(args: Array[String]): Unit = T2xJob.main(T2xJob.T2sMode, args)
}

/**
 * Spark job — stdtime → stdcount (equivalent to the t2c binary).
 *
 *   T2C_IDIR   S3 path to input stdtime file (required)
 *   T2C_ODIR   S3 path for output stdcount file (required)
 */
object T2cJob {
  def main(args: Array[String]): Unit = T2xJob.main(T2xJob.T2cMode, args)
}
