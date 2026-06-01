#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <unistd.h>
#include "FD_Calculus.h"

using namespace std;

extern "C" {

/*
 * JNI bridge for mix_maprule logic.
 *
 * Called from Scala as: FDNative.runMix(configLines, inputDir, outputDir, suffix, thinningPct)
 *
 * Parameters (all passed as Java strings, converted to C strings here):
 *   jConfigLines  - newline-separated config commands, e.g.:
 *                     "mm1 add 1.0\nw143 add 1.0"
 *   jInputDir     - local directory containing stdtime.* input files
 *   jOutputDir    - local directory to write output stdtime.* files
 *   jSuffix       - output filename suffix (e.g. "mix1")
 *   thinningPct   - thinning percentage jump (same as mix_maprule -j); 0.0 = no thinning
 *
 * Returns 0 on success, non-zero on failure.
 *
 * Function name format: Java_<packageName>_<ClassName>_<methodName>
 * Must match exactly what is declared @native in FDNative.scala
 */
JNIEXPORT jint JNICALL
Java_com_fd_FDNative_runMix(
    JNIEnv  *env,
    jobject  obj,
    jstring  jConfigLines,
    jstring  jInputDir,
    jstring  jOutputDir,
    jstring  jSuffix,
    jdouble  thinningPct)
{
    // Step 1: Convert Java strings to C strings
    const char *configLines = env->GetStringUTFChars(jConfigLines, 0);
    const char *inputDir    = env->GetStringUTFChars(jInputDir,    0);
    const char *outputDir   = env->GetStringUTFChars(jOutputDir,   0);
    const char *suffix      = env->GetStringUTFChars(jSuffix,      0);

    // Step 2: Write config lines to a temp file so readCommands() can parse it
    // (readCommands expects a file path, not an in-memory string)
    char tmpPath[] = "/tmp/fd_config_XXXXXX";
    int fd = mkstemp(tmpPath);
    if (fd < 0) {
        fprintf(stderr, "FD_JNI: failed to create temp file\n");
        env->ReleaseStringUTFChars(jConfigLines, configLines);
        env->ReleaseStringUTFChars(jInputDir,    inputDir);
        env->ReleaseStringUTFChars(jOutputDir,   outputDir);
        env->ReleaseStringUTFChars(jSuffix,      suffix);
        return 1;
    }
    FILE *f = fdopen(fd, "w");
    fprintf(f, "%s", configLines);
    fclose(f);

    // Step 3: Run the same logic as mix_maprule main()
    FD_Calculus_Config fcc;
    fcc.setInputDirectory((char*)inputDir);
    fcc.setOutputDirectory((char*)outputDir);
    fcc.setAnswerSuffix(string(suffix));
    if (thinningPct > 0.0) {
        fcc.setThinningPctJump(thinningPct);  // equivalent to mix_maprule -j <pct>
    }

    int ret = 1;
    if (fcc.readCommands(tmpPath) == 0) {
        if (fcc.process() == 0) {
            ret = fcc.saveAnswerFD();
        }
    }

    // Step 4: Clean up temp file and release Java strings
    unlink(tmpPath);
    env->ReleaseStringUTFChars(jConfigLines, configLines);
    env->ReleaseStringUTFChars(jInputDir,    inputDir);
    env->ReleaseStringUTFChars(jOutputDir,   outputDir);
    env->ReleaseStringUTFChars(jSuffix,      suffix);

    return (jint)ret;
}

/*
 * JNI bridge for unshard logic.
 *
 * Called from Scala as: FDNative.runUnshard(inputFile, outputFile, factor)
 *
 * Equivalent to running:  unshard <inputFile> <factor> <outputFile>
 *
 * Reads a stdtime file, applies the unshard factor, writes the result.
 *
 * Parameters:
 *   jInputFile   - local path to the stdtime input file
 *   jOutputFile  - local path to write the unsharded stdtime output
 *   factor       - unshard factor (float, same as unshard's argv[2])
 *
 * Returns 0 on success, non-zero on failure.
 */
JNIEXPORT jint JNICALL
Java_com_fd_FDNative_runUnshard(
    JNIEnv  *env,
    jobject  obj,
    jstring  jInputFile,
    jstring  jOutputFile,
    jfloat   factor)
{
    const char *inputFile  = env->GetStringUTFChars(jInputFile,  0);
    const char *outputFile = env->GetStringUTFChars(jOutputFile, 0);

    int ret = 0;
    try {
        FD a;
        a.stdtime.read((char*)inputFile);
        a.stdtime.unshard(factor);
        a.stdtime.write((char*)outputFile);
    } catch (...) {
        fprintf(stderr, "FD_JNI: runUnshard threw an exception\n");
        ret = 1;
    }

    env->ReleaseStringUTFChars(jInputFile,  inputFile);
    env->ReleaseStringUTFChars(jOutputFile, outputFile);

    return (jint)ret;
}

/*
 * JNI bridge for t2s (stdtime -> stdspace) logic.
 *
 * Called from Scala as: FDNative.runT2s(inputFile, outputFile)
 *
 * Equivalent to running:  t2s <inputFile> <outputFile>
 *
 * Reads a stdtime file, computes stdspace via createStdspaceStdcount(false),
 * writes the resulting stdspace file.
 *
 * Returns 0 on success, non-zero on failure.
 */
JNIEXPORT jint JNICALL
Java_com_fd_FDNative_runT2s(
    JNIEnv  *env,
    jobject  obj,
    jstring  jInputFile,
    jstring  jOutputFile)
{
    const char *inputFile  = env->GetStringUTFChars(jInputFile,  0);
    const char *outputFile = env->GetStringUTFChars(jOutputFile, 0);

    int ret = 0;
    try {
        FD a;
        a.stdtime.read((char*)inputFile);
        a.createStdspaceStdcount(false);
        a.stdspace.write((char*)outputFile);
    } catch (...) {
        fprintf(stderr, "FD_JNI: runT2s threw an exception\n");
        ret = 1;
    }

    env->ReleaseStringUTFChars(jInputFile,  inputFile);
    env->ReleaseStringUTFChars(jOutputFile, outputFile);

    return (jint)ret;
}

/*
 * JNI bridge for t2c (stdtime -> stdcount) logic.
 *
 * Called from Scala as: FDNative.runT2c(inputFile, outputFile)
 *
 * Equivalent to running:  t2c <inputFile> <outputFile>
 *
 * Reads a stdtime file, computes stdcount via createStdspaceStdcount(false),
 * writes the resulting stdcount file.
 *
 * Returns 0 on success, non-zero on failure.
 */
JNIEXPORT jint JNICALL
Java_com_fd_FDNative_runT2c(
    JNIEnv  *env,
    jobject  obj,
    jstring  jInputFile,
    jstring  jOutputFile)
{
    const char *inputFile  = env->GetStringUTFChars(jInputFile,  0);
    const char *outputFile = env->GetStringUTFChars(jOutputFile, 0);

    int ret = 0;
    try {
        FD a;
        a.stdtime.read((char*)inputFile);
        a.createStdspaceStdcount(false);
        a.stdcount.write((char*)outputFile);
    } catch (...) {
        fprintf(stderr, "FD_JNI: runT2c threw an exception\n");
        ret = 1;
    }

    env->ReleaseStringUTFChars(jInputFile,  inputFile);
    env->ReleaseStringUTFChars(jOutputFile, outputFile);

    return (jint)ret;
}

} // extern "C"
