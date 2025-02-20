val scala2Version = "2.13.15"

lazy val root = project
  .in(file("."))
  .settings(
    organization := "io.armadaproject.armada",
    name := "Scala Armada Client Example",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala2Version,

    libraryDependencies += "io.armadaproject.armada" %% "scala-armada-client" % "0.1.0-SNAPSHOT",
    libraryDependencies += "org.scalameta" %% "munit" % "1.0.0" % Test
  )

Compile / PB.targets := Seq(
  scalapb.gen() -> (Compile / sourceManaged).value
)

// Additional directories to search for imports:
Compile / PB.protoSources ++= Seq(file("./proto"))

libraryDependencies ++= Seq(
    "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion
)

resolvers += Resolver.mavenLocal
