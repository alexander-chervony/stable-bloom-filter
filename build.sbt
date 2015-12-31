val guava = "com.google.guava" % "guava" % "19.0"
val scalatest = "org.scalatest" %% "scalatest" % "2.2.4" % "test"

lazy val root = (project in file(".")).
    settings(
      name := "stable bloom filter",
      version := "1.0",
      scalaVersion := "2.11.7",
      libraryDependencies ++= Seq(guava, scalatest)
    )