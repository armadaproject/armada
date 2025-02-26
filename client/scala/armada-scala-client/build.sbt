val scala2Version = "2.13.15"

lazy val root = project
  .in(file("."))
  .settings(
    organization := "io.armadaproject.armada",
    name := "Armada Scala Client",
    version := "0.1.0-SNAPSHOT",
    versionScheme := Some("semver-spec"),

    organization := "io.armadaproject.armada",
    organizationName := "Armada Project",
    organizationHomepage := Some(url("https://armadaproject.io/")),
    description := "The Armada Scala client.",
    licenses := List(
      "Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")
    ),
    homepage := Some(url("https://github.com/armadaproject/armada")),
    scmInfo := Some(
      ScmInfo(
        url("https://github.com/armadaproject/armada"),
        "scm:git@github.com:armadaproject/armada.git"
      )
    ),

    developers := List(
      Developer(
        id = "richscott",
        name = "Rich Scott",
        email = "rich@gr-oss.io",
        url = url("https://github.com/richscott/")
      ),
      Developer(
        id = "EnricoMi",
        name = "Enrico Minack",
        email = "github@enrico.minack.dev",
        url = url("https://github.com/EnricoMi/")
      )
    ),

    scalaVersion := scala2Version,
    crossScalaVersions := List(scala2Version, "2.12.18"),

    libraryDependencies += "org.scalameta" %% "munit" % "1.0.0" % Test
  )

Compile / PB.targets := Seq(
  scalapb.gen() -> (Compile / sourceManaged).value
)

// Additional directories to search for imports:
Compile / PB.protoSources ++= Seq(file("./proto"))

libraryDependencies ++= Seq(
    "io.grpc" % "grpc-netty" % scalapb.compiler.Version.grpcJavaVersion,
    "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
    "com.github.jkugiya" %% "ulid-scala" % "1.0.5"
)
