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
Compile / PB.protoSources ++= Seq(file("./proto"))

// Exclude Armada test examples
// Compile / packageBin / mappings := {
//   val originalMappings = (Compile / packageBin / mappings).value
//   originalMappings.filter { case (file, name) =>
//     !name.startsWith("io/armadaproject/armada/")
//   }
// }

libraryDependencies ++= Seq(
    "io.grpc" % "grpc-netty" % scalapb.compiler.Version.grpcJavaVersion,
    "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion
)
