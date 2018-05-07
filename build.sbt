
name := "taxi-rides-gen"

organization := "svend.taxirides"
scalaVersion := "2.12.6"
version      := "0.1.0-SNAPSHOT"


libraryDependencies ++= Seq(
        "org.scalatest" %% "scalatest" % "3.0.5" % Test,
        "org.scalacheck" %% "scalacheck" % "1.14.0" % Test,
        "com.lightbend" %% "kafka-streams-scala" % "0.2.1",
        "org.slf4j" % "slf4j-simple" % "1.7.25"
)

resolvers ++= Seq(
  "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
   Resolver.sonatypeRepo("public")
)

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true
