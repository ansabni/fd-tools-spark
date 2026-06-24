name         := "fd-spark"
version      := "0.1"
scalaVersion := "2.13.17"

// Spark 4.0.0 is "provided" on a real cluster — bundled for local dev runs
val sparkScope = "provided" // change to "compile" temporarily to run via `sbt runMain`

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"             % "4.0.0"   % sparkScope,
  "org.apache.spark" %% "spark-sql"              % "4.0.0"   % sparkScope,
  // S3A support for Linode Object Storage (provided at runtime via --packages)
  "org.apache.hadoop" %  "hadoop-aws"            % "3.3.6"   % "provided",
  "com.amazonaws"     %  "aws-java-sdk-bundle"   % "1.12.262" % "provided"
)

assembly / assemblyJarName := "fd-tools-spark-assembly.jar"

// Make "provided" deps visible on the runtime classpath when using `sbt run`
// (they are excluded from the assembly jar — only needed for local testing)
Compile / run / unmanagedClasspath ++= (Compile / dependencyClasspath).value
  .filter(_.data.name.contains("spark"))

// When running locally via `sbt run`, include Spark (normally "provided")
run / fork        := true
run / javaOptions ++= Seq(
  s"-Djava.library.path=${baseDirectory.value}/lib",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",  // required by Spark 4 on Java 21
  "--add-opens=java.base/java.lang=ALL-UNNAMED"
)

// When running tests
Test / fork        := true
Test / javaOptions ++= Seq(
  s"-Djava.library.path=${baseDirectory.value}/lib",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED"
)

// ---------------------------------------------------------------------------
// Bundle native libs into the jar as resources under "native/".
// Picks up whichever of libFD.so / libFD.dylib exist in lib/.
// FDNative extracts them at runtime via getResourceAsStream + System.load().
// ---------------------------------------------------------------------------
Compile / resourceGenerators += Def.task {
  val libDir = baseDirectory.value / "lib"
  val outDir = (Compile / resourceManaged).value / "native"
  IO.createDirectory(outDir)
  val candidates = Seq("libFD.so", "libFD.dylib")
  candidates.flatMap { name =>
    val src = libDir / name
    if (src.exists() && !java.nio.file.Files.isSymbolicLink(src.toPath)) {
      val dst = outDir / name
      IO.copyFile(src, dst)
      streams.value.log.info(s"[native] bundling $src → resource native/$name")
      Some(dst)
    } else if (src.exists()) {
      // Resolve the symlink and bundle the real file
      val real = java.nio.file.Files.readSymbolicLink(src.toPath).toFile
      val resolved = if (real.isAbsolute) real else new File(libDir, real.getPath)
      if (resolved.exists()) {
        val dst = outDir / name
        IO.copyFile(resolved, dst)
        streams.value.log.info(s"[native] bundling (via symlink) $resolved → resource native/$name")
        Some(dst)
      } else None
    } else None
  }
}.taskValue
