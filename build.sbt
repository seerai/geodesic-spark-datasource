// Geodesic Spark DataSource for Apache Sedona

ThisBuild / organization := "ai.seer"
ThisBuild / scalaVersion := "2.12.13"
ThisBuild / version := sys.env.getOrElse("VERSION", "0.0.1-SNAPSHOT")

lazy val root = (project in file(".")).settings(
  name := "geodesic-spark-datasource-sedona",
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
  javaOptions ++= Seq("-Xms512M", "-Xmx2048M"),
  scalacOptions ++= Seq("-deprecation", "-unchecked"),
  Test / parallelExecution := false,
  fork := true,
  coverageHighlighting := true,
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-streaming" % "3.3.0" % "provided",
    "org.apache.spark" %% "spark-sql" % "3.3.0" % "provided",
    "com.softwaremill.sttp.client3" %% "core" % "3.10.3",
    "org.playframework" %% "play-json" % "3.0.4",
    "org.locationtech.geomesa" %% "geomesa-spark-jts" % "5.2.0",
    "org.scalatest" %% "scalatest" % "3.2.2" % "test",
    "org.scalacheck" %% "scalacheck" % "1.15.2" % "test",
    "com.holdenkarau" %% "spark-testing-base" % "3.3.0_1.3.0" % "test",
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.17.1",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.17.1",
    "org.apache.sedona" % "sedona-spark-shaded-3.3_2.12" % "1.7.1" % "provided"
  ),

  // uses compile classpath for the run task, including "provided" jar (cf http://stackoverflow.com/a/21803413/3827)
  Compile / run := Defaults
    .runTask(
      Compile / fullClasspath,
      Compile / run / mainClass,
      Compile / run / runner
    )
    .evaluated,
  scalacOptions ++= Seq("-deprecation", "-unchecked"),
  pomIncludeRepository := { x => false },
  resolvers ++= Seq(
    "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
    "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/",
    "Second Typesafe repo" at "https://repo.typesafe.com/typesafe/maven-releases/"
  ) ++ Resolver.sonatypeOssRepos("public"),
  pomIncludeRepository := { _ => false },

  // Maven Central publishing configuration (Central Publisher Portal)
  publishTo := Some(
    "Central Repository" at "https://central.sonatype.com/api/v1/publisher/upload"
  ),

  // Required metadata for Maven Central
  description := "Spark DataSource v2 for accessing Geodesic spatial data with Apache Sedona integration",
  homepage := Some(url("https://github.com/seerai/geodesic-spark-datasource")),
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/seerai/geodesic-spark-datasource"),
      "scm:git@github.com:seerai/geodesic-spark-datasource.git"
    )
  ),
  developers := List(
    Developer(
      id = "seerai",
      name = "Seer AI",
      email = "contact@seerai.space",
      url = url("https://seerai.space")
    )
  ),
  licenses := List(
    "Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt")
  ),

  // Publishing settings
  publishMavenStyle := true,
  Test / publishArtifact := false,
  pomIncludeRepository := { _ => false },

  // Ensure sources and docs are published
  Compile / packageDoc / publishArtifact := true,
  Compile / packageSrc / publishArtifact := true
)
