organization  := "io.ogmios"

name := "ogmios-core"

version := "0.1-SNAPSHOT"

scalaVersion := "2.10.3"

resolvers ++= Seq(
  "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/",
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
)

libraryDependencies ++= Seq(
  "com.datastax.cassandra"  % "cassandra-driver-core" % "2.0.1",
  "org.xerial.snappy"       % "snappy-java"           % "1.0.5",
  "com.typesafe.akka" %% "akka-actor" % "2.3.2",
  "com.typesafe.akka" %% "akka-kernel" % "2.3.2",
  "com.typesafe.akka" %% "akka-testkit" % "2.3.2"%"test",
  "ch.qos.logback" % "logback-classic" % "1.0.9",
  "org.scalacheck" %% "scalacheck" % "1.10.1" % "test",
  "org.scalatest" % "scalatest_2.10" % "2.1.0" % "test"
)

    