package com.fd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.security.MessageDigest
import java.util.Base64

import scala.collection.mutable
import scala.io.Source

/**
 * Batch t2s job — converts ALL stdtime.* files in an S3 prefix to stdspace.*.
 *
 * One Spark task per input file; tasks run in parallel across executors.
 *
 * Env vars:
 *   T2S_INPUT_PREFIX   S3 prefix (no trailing /) containing stdtime.* files  (required)
 *   T2S_OUTPUT_PREFIX  S3 prefix for output stdspace.* files                 (required)
 *
 * Credentials / encryption:
 *   S3_ACCESS_KEY, S3_SECRET_KEY, S3_ENDPOINT
 *   S3_OUTPUT_KEY_INDEX  Key index for output SSE-C encryption (default: highest available)
 *
 * Usage:
 *   spark-submit --class com.fd.BatchT2sJob fd-spark_2.13-0.1.jar <s3Bucket> <encKeysFile>
 */
object BatchT2sJob {

  def s3Endpoint: String = sys.env.getOrElse("S3_ENDPOINT", "us-ord-10.linodeobjects.com")

  def keyIndexFromPath(path: String): String = {
    val m = """\.key(\d+)$""".r.findFirstMatchIn(path)
    m.map(_.group(1)).getOrElse("2")
  }

  def stripKeyN(name: String): String = name.replaceAll("""\.key\d+$""", "")

  def loadEncryptionKeys(path: String): Map[String, String] = {
    val content = Source.fromFile(path).mkString
    val pattern = """"(\w+)"\s*:\s*"([^"]+)"""".r
    pattern.findAllMatchIn(content).map(m => m.group(1) -> m.group(2)).toMap
  }

  def s3aConf(accessKey: String, secretKey: String, sseKeyB64: String,
              withSseC: Boolean = true): Configuration = {
    val conf = new Configuration(false)
    conf.set("fs.s3a.impl",                "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("fs.s3a.endpoint",            s3Endpoint)
    conf.set("fs.s3a.path.style.access",   "true")
    conf.set("fs.s3a.connection.ssl.enabled", "true")
    conf.set("fs.s3a.signing-algorithm",   "S3SignerType")
    conf.set("fs.s3a.checksum.validation", "false")
    conf.set("fs.s3a.access.key",          accessKey)
    conf.set("fs.s3a.secret.key",          secretKey)
    if (withSseC) {
      conf.set("fs.s3a.server-side-encryption-algorithm", "SSE-C")
      conf.set("fs.s3a.server-side-encryption.key",       sseKeyB64)
      val keyBytes = Base64.getDecoder.decode(sseKeyB64)
      val md5B64   = Base64.getEncoder.encodeToString(
                       MessageDigest.getInstance("MD5").digest(keyBytes))
      conf.set("fs.s3a.server-side-encryption-key-md5", md5B64)
    }
    conf.set("hadoop.tmp.dir", System.getProperty("java.io.tmpdir", "/tmp"))
    conf
  }

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

  def downloadS3File(s3Path: String, localFile: java.io.File, conf: Configuration): Unit = {
    val path = new Path(s3Path)
    val fs   = FileSystem.newInstance(path.toUri, conf)
    val in   = fs.open(path)
    val out  = new java.io.FileOutputStream(localFile)
    try {
      val buf = new Array[Byte](64 * 1024)
      var n   = in.read(buf)
      while (n > 0) { out.write(buf, 0, n); n = in.read(buf) }
    } finally { in.close(); out.close(); fs.close() }
  }

  def uploadToS3(localFile: java.io.File, s3Path: String, conf: Configuration): Unit = {
    val path = new Path(s3Path)
    val fs   = FileSystem.newInstance(path.toUri, conf)
    val out  = fs.create(path, /*overwrite=*/ true)
    val in   = new java.io.FileInputStream(localFile)
    try {
      val buf = new Array[Byte](64 * 1024)
      var n   = in.read(buf)
      while (n > 0) { out.write(buf, 0, n); n = in.read(buf) }
    } finally { in.close(); out.close(); fs.close() }
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        "Usage: BatchT2sJob <s3Bucket> <encryptionKeysFile>\n" +
        "Env vars: T2S_INPUT_PREFIX, T2S_OUTPUT_PREFIX, S3_ACCESS_KEY, S3_SECRET_KEY"
      )
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("BatchT2sJob")
    if (!conf.contains("spark.master")) conf.setMaster("local[*]")
    val sc = new SparkContext(conf)
    try {
      run(sc, args(0), args(1))
      println("BatchT2sJob complete.")
    } finally sc.stop()
  }

  def run(sc: SparkContext, s3Bucket: String, encryptionKeysFile: String): Unit = {

    val accessKey = sys.env.getOrElse("S3_ACCESS_KEY",
      throw new IllegalStateException("S3_ACCESS_KEY not set"))
    val secretKey = sys.env.getOrElse("S3_SECRET_KEY",
      throw new IllegalStateException("S3_SECRET_KEY not set"))

    val inputPrefix = sys.env.getOrElse("T2S_INPUT_PREFIX",
      throw new IllegalStateException("T2S_INPUT_PREFIX not set")).stripSuffix("/")
    val outputPrefix = sys.env.getOrElse("T2S_OUTPUT_PREFIX",
      throw new IllegalStateException("T2S_OUTPUT_PREFIX not set")).stripSuffix("/")

    val encryptionKeys = loadEncryptionKeys(encryptionKeysFile)
    println(s"[BatchT2s] Loaded ${encryptionKeys.size} key(s)")

    val outKeyIdx = sys.env.get("S3_OUTPUT_KEY_INDEX").filter(_.nonEmpty)
      .getOrElse(encryptionKeys.keys.toSeq.sorted.last)

    val listConf = s3aConf(accessKey, secretKey, encryptionKeys(encryptionKeys.keys.min),
                           withSseC = false)
    val allFiles = listS3Files(s3Bucket, inputPrefix, listConf)
      .filter(p => p.split("/").last.replaceAll("""\.key\d+$""", "").startsWith("stdtime"))

    println(s"[BatchT2s] Found ${allFiles.size} stdtime file(s) under s3://$s3Bucket/$inputPrefix")
    println(s"[BatchT2s] Output prefix: s3://$s3Bucket/$outputPrefix")
    println(s"[BatchT2s] Output SSE-C key index: $outKeyIdx")

    val bcAccessKey      = sc.broadcast(accessKey)
    val bcSecretKey      = sc.broadcast(secretKey)
    val bcEncryptionKeys = sc.broadcast(encryptionKeys)
    val bcBucket         = sc.broadcast(s3Bucket)
    val bcOutputPrefix   = sc.broadcast(outputPrefix)
    val bcOutKeyIdx      = sc.broadcast(outKeyIdx)

    val results: Array[(String, Int)] = sc.parallelize(allFiles, allFiles.size.max(1))
      .map { s3Path =>

        val ak       = bcAccessKey.value
        val sk       = bcSecretKey.value
        val keys     = bcEncryptionKeys.value
        val bucket   = bcBucket.value
        val outPfx   = bcOutputPrefix.value
        val outIdx   = bcOutKeyIdx.value

        val rawName    = s3Path.split("/").last
        val baseName   = stripKeyN(rawName)           // e.g. "stdtime.b^f^VCD_10102_ExaBot"
        val suffix     = baseName.stripPrefix("stdtime.")  // e.g. "b^f^VCD_10102_ExaBot"
        val outName    = s"stdspace.$suffix"           // e.g. "stdspace.b^f^VCD_10102_ExaBot"
        val inKeyIdx   = keyIndexFromPath(rawName)

        val taskId   = java.util.UUID.randomUUID().toString
        val localDir = new java.io.File(s"/tmp/fd_t2s_$taskId")
        localDir.mkdirs()
        val localIn  = new java.io.File(localDir, baseName)
        val localOut = new java.io.File(localDir, outName)

        println(s"[t2s/$suffix] Downloading $rawName (key $inKeyIdx)")
        try {
          val dlConf = s3aConf(ak, sk, keys(inKeyIdx))
          downloadS3File(s3Path, localIn, dlConf)
          println(s"[t2s/$suffix]   ${localIn.length()} bytes")

          println(s"[t2s/$suffix] Running t2s via JNI...")
          val ret = FDNative.runT2s(localIn.getAbsolutePath, localOut.getAbsolutePath)
          if (ret != 0 || !localOut.exists()) {
            System.err.println(s"[t2s/$suffix] ERROR: runT2s returned $ret, output exists=${localOut.exists()}")
            (suffix, if (ret != 0) ret else 1)
          } else {
            println(s"[t2s/$suffix]   stdspace size: ${localOut.length()} bytes")
            val dest = s"s3a://$bucket/$outPfx/${outName}.key$outIdx"
            println(s"[t2s/$suffix] Uploading → $dest")
            val upConf = s3aConf(ak, sk, keys(outIdx))
            uploadToS3(localOut, dest, upConf)
            println(s"[t2s/$suffix] Done ✓")
            (suffix, 0)
          }
        } finally {
          def delRec(f: java.io.File): Unit = {
            if (f.isDirectory) Option(f.listFiles()).getOrElse(Array()).foreach(delRec)
            f.delete()
          }
          delRec(localDir)
        }
      }
      .collect()

    val ok     = results.count(_._2 == 0)
    val failed = results.filter(_._2 != 0)
    println(s"\n=== BatchT2sJob: $ok/${results.length} succeeded, ${failed.length} failed ===")
    if (failed.nonEmpty) {
      println("Failed files:"); failed.foreach { case (s, c) => println(s"  $s (code $c)") }
      throw new RuntimeException(s"BatchT2sJob: ${failed.length} task(s) failed")
    }
  }
}
