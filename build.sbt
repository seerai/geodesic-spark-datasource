// give the user a nice default project!

val sparkVersion = settingKey[String]("Spark version")

lazy val root = (project in file(".")).settings(
  inThisBuild(
    List(
      organization := "com.seerai",
      scalaVersion := "2.12.13"
    )
  ),
  name := "geodesic",
  version := "0.0.1",
  sparkVersion := "3.3.0",
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
  javaOptions ++= Seq("-Xms512M", "-Xmx2048M"),
  scalacOptions ++= Seq("-deprecation", "-unchecked"),
  parallelExecution in Test := false,
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
  run in Compile := Defaults
    .runTask(
      fullClasspath in Compile,
      mainClass in (Compile, run),
      runner in (Compile, run)
    )
    .evaluated,
  scalacOptions ++= Seq("-deprecation", "-unchecked"),
  pomIncludeRepository := { x => false },
  resolvers ++= Seq(
    "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
    "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/",
    "Second Typesafe repo" at "https://repo.typesafe.com/typesafe/maven-releases/",
    Resolver.sonatypeRepo("public")
  ),
  pomIncludeRepository := { _ => false },

  // publish settings
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
  }
)
