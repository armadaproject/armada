val scala2Version = "2.13.15"

lazy val root = project
  .in(file("."))
  .settings(
    name := "Scala Armada Client",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala2Version,

    libraryDependencies += "org.scalameta" %% "munit" % "1.0.0" % Test
  )

Compile / PB.targets := Seq(
  scalapb.gen() -> (Compile / sourceManaged).value
)

// Additional directories to search for imports:
// Compile / PB.includePaths ++= Seq(file("../../../proto"), file("../../../proto/armada"))
// Compile / PB.protoSources ++= Seq(file("../../../proto"))

libraryDependencies ++= Seq(
    "io.grpc" % "grpc-netty" % scalapb.compiler.Version.grpcJavaVersion,
    "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion
)
