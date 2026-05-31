name         := "fd-spark"
version      := "0.1"
scalaVersion := "2.13.17"

// Spark 4.1.1 is "provided" on a real cluster — bundled for local dev runs
val sparkScope = "provided" // change to "compile" temporarily to run via `sbt runMain`

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"             % "4.1.1"   % sparkScope,
  "org.apache.spark" %% "spark-sql"              % "4.1.1"   % sparkScope,
  // S3A support for Linode Object Storage (provided at runtime via --packages)
  "org.apache.hadoop" %  "hadoop-aws"            % "3.3.6"   % "provided",
  "com.amazonaws"     %  "aws-java-sdk-bundle"   % "1.12.262" % "provided"
)

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
