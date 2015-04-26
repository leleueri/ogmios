name := "ogmios"

version := "1.0"

organization := "com.github.leleueri"

scalaVersion := "2.11.6"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

resolvers += "TimeUUID for Cassandra" at "http://eaio.com/maven2"

libraryDependencies ++= {
  val akkaV = "2.3.9"
  val akkaStreamV = "1.0-M5"
  val scalaTestV = "2.2.1"
  Seq(
    "com.typesafe.akka" %% "akka-actor" % akkaV,
    "com.typesafe.akka" %% "akka-stream-experimental" % akkaStreamV,
    "com.typesafe.akka" %% "akka-http-core-experimental" % akkaStreamV,
    "com.typesafe.akka" %% "akka-http-experimental" % akkaStreamV,
    "com.typesafe.akka" %% "akka-http-spray-json-experimental" % akkaStreamV,
    "com.typesafe.akka" %% "akka-http-testkit-experimental" % akkaStreamV,

    "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.4",
    "com.eaio.uuid" % "uuid" % "3.4",

    "org.scalatest" %% "scalatest" % scalaTestV % "test"
  )
}
