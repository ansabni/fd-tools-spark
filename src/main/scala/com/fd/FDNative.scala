package com.fd

/**
 * JNI wrapper around the C++ FD library (libFD.dylib / libFD.so).
 *
 * @native methods MUST live in a class (not a Scala object) so the JVM
 * looks for the symbol  Java_com_fd_FDNative_runMix  (no _00024 dollar
 * suffix that a Scala object/companion would require).
 *
 * Use the companion object `FDNative` for convenient static-style access;
 * it extends this class and triggers library loading on first touch.
 */
class FDNative {
  /**
   * Runs the FD mix logic natively (equivalent to running mix_maprule).
   *
   * @param configLines   Newline-separated config commands, one per line:
   *                        "<trafficClass> add|sub|concat <scale>"
   * @param inputDir      Local directory containing stdtime.* input files
   * @param outputDir     Local directory to write output stdtime.* files
   * @param suffix        Output filename suffix (e.g. "mymix")
   * @param thinningPct   Thinning percentage jump (same as mix_maprule -j); 0.0 = no thinning
   * @return              0 on success, non-zero on failure
   */
  @native def runMix(
    configLines:  String,
    inputDir:     String,
    outputDir:    String,
    suffix:       String,
    thinningPct:  Double = 0.0
  ): Int

  /**
   * Runs the FD unshard logic natively (equivalent to running the unshard binary).
   *
   * Reads a stdtime file, applies the unshard factor, writes the result.
   *
   * @param inputFile   Local path to the stdtime input file
   * @param outputFile  Local path to write the unsharded stdtime output
   * @param factor      Unshard factor (float; same as unshard's 2nd argv)
   * @return            0 on success, non-zero on failure
   */
  @native def runUnshard(
    inputFile:  String,
    outputFile: String,
    factor:     Float
  ): Int

  /**
   * Runs the FD t2s logic natively (stdtime → stdspace, equivalent to the t2s binary).
   *
   * @param inputFile   Local path to the stdtime input file
   * @param outputFile  Local path to write the stdspace output
   * @return            0 on success, non-zero on failure
   */
  @native def runT2s(
    inputFile:  String,
    outputFile: String
  ): Int

  /**
   * Runs the FD t2c logic natively (stdtime → stdcount, equivalent to the t2c binary).
   *
   * @param inputFile   Local path to the stdtime input file
   * @param outputFile  Local path to write the stdcount output
   * @return            0 on success, non-zero on failure
   */
  @native def runT2c(
    inputFile:  String,
    outputFile: String
  ): Int

  /**
   * Allocates a C++ ReducerContext (splay tree + all accumulator state)
   * for one Spark partition. Returns an opaque Long handle that must be
   * passed to every subsequent reducerPushBatch / reducerFinalize call.
   *
   * @param outputDir  Directory where stdtime.<extension> will be written
   * @param extension  stdtime file suffix (e.g. "7" for serial 7)
   * @return           Opaque handle (C++ pointer cast to Long)
   */
  @native def reducerInit(outputDir: String, extension: String): Long

  /**
   * Processes one batch of sorted log lines. Called repeatedly until all
   * lines for the partition have been fed through.
   *
   * @param handle     Handle returned by reducerInit
   * @param batch      Newline-separated formatted lines:
   *                   "<key>:<ts>:<md5>:<size>:<bytes>:<cpcode>:<serial>:<max_age>"
   * @return           0 on success, non-zero on error
   */
  @native def reducerPushBatch(handle: Long, batch: String): Int

  /**
   * Finalizes the reducer: writes stdtime.<extension> to outputDir,
   * then frees all C++ memory for this context.
   * Must be called exactly once after all batches have been pushed.
   *
   * @param handle     Handle returned by reducerInit
   * @return           0 on success, non-zero on error
   */
  @native def reducerFinalize(handle: Long): Int
}

object FDNative extends FDNative {
  // ---------------------------------------------------------------------------
  // Native library loading.
  //
  // Resolution order:
  //   1. Bundled jar resource   /native/libFD.{so,dylib}    ← preferred
  //   2. java.library.path / LD_LIBRARY_PATH (System.loadLibrary)  ← fallback
  //
  // The build.sbt resourceGenerators task copies lib/libFD.{so,dylib} into the
  // packaged jar under "native/" so the receiving team only needs the jar.
  // ---------------------------------------------------------------------------
  private def platformLibName(): String = {
    val os = System.getProperty("os.name", "").toLowerCase
    if (os.contains("mac") || os.contains("darwin")) "libFD.dylib"
    else                                             "libFD.so"
  }

  private def loadFromJar(): Boolean = {
    val name         = platformLibName()
    val resourcePath = s"/native/$name"
    val in           = getClass.getResourceAsStream(resourcePath)
    if (in == null) {
      System.err.println(s"[FDNative] no $resourcePath in jar — falling back to java.library.path")
      return false
    }
    try {
      val dotIdx = name.lastIndexOf('.')
      val (prefix, suffix) =
        if (dotIdx > 0) (name.substring(0, dotIdx), name.substring(dotIdx))
        else            (name, "")
      val tmp = java.io.File.createTempFile(prefix, suffix)
      tmp.deleteOnExit()
      java.nio.file.Files.copy(
        in, tmp.toPath,
        java.nio.file.StandardCopyOption.REPLACE_EXISTING
      )
      System.load(tmp.getAbsolutePath)
      System.err.println(s"[FDNative] Extracted $name → ${tmp.getAbsolutePath}")
      System.err.println(s"[FDNative] Loaded native library successfully ✓")
      true
    } catch {
      case _: UnsatisfiedLinkError =>
        System.err.println(s"[FDNative] Bundled $name is not compatible with this platform — falling back to java.library.path")
        false
    } finally in.close()
  }

  if (!loadFromJar()) {
    System.loadLibrary("FD")
  }
}
