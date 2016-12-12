name := "s3-indexer"
version := "1.0"
scalaVersion := "2.10.5"


libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk" % "1.9.16",
  "com.amazonaws" % "aws-lambda-java-core" % "1.1.0",
  ("org.apache.spark" %% "spark-core" % "1.6.0").
    exclude("org.mortbay.jetty","servlet-api").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("commons-collections", "commons-collections").
    exclude("commons-logging", "commons-logging").
    exclude("com.google.guava", "guava").
    exclude("com.esotericsoftware.minlog", "minlog"),
  "org.json" % "json" % "20090211"
)

mainClass in (Compile, run) := Some("com.aws.hli.S3Index")

