organization  := "io.ogmios"

version       := "0.1"

scalaVersion  := "2.10.3"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")


resolvers ++= Seq(
  "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/",
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
)

libraryDependencies ++= {
  val akkaV = "2.3.0"
  val sprayV = "1.3.1"
  Seq(
    "com.datastax.cassandra"  % "cassandra-driver-core" % "2.0.1",
    "org.xerial.snappy"       % "snappy-java"           % "1.0.5",
    "io.spray"            %   "spray-can"     % sprayV,
    "io.spray"            %   "spray-routing" % sprayV,
    "io.spray"            %   "spray-httpx"   % sprayV,
    "io.spray"            %   "spray-testkit" % sprayV  % "test",
    "com.typesafe.akka"   %%  "akka-actor"    % akkaV,
    "com.typesafe.akka"   %%  "akka-testkit"  % akkaV   % "test",
    "org.specs2"          %%  "specs2-core"   % "2.3.7" % "test",
    "ch.qos.logback" % "logback-classic" % "1.0.9",
    "org.scalacheck" %% "scalacheck" % "1.10.1" % "test",
    "org.scalatest" % "scalatest_2.10" % "2.1.0" % "test",
    "io.spray" %%  "spray-json" % "1.2.6"
   )
}

Revolver.settings

