ThisBuild / scalaVersion     := "2.12.9"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "dev.zio"
ThisBuild / organizationName := "workshop"

lazy val root = (project in file("."))
  .settings(
    name := "ZIO Workshop",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % "1.0.0-RC12-1",
      "dev.zio" %% "zio-streams" % "1.0.0-RC12-1"
    ),
    scalafmtOnCompile := true
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
