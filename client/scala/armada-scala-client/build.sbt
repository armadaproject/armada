val scala2Version = "2.13.15"

lazy val root = project
  .in(file("."))
  .settings(
    name := "Armada Scala Client",
    description := "The Armada Scala client.",

    version := "0.1.0-SNAPSHOT",
    versionScheme := Some("semver-spec"),

    organization := "io.armadaproject.armada",
    organizationName := "Armada Project",
    organizationHomepage := Some(url("https://armadaproject.io/")),

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
        id = "ClifHouck",
        name = "Clif Houck",
        email = "me@clifhouck.com",
        url = url("https://github.com/ClifHouck/")
      ),
      Developer(
        id = "dejanzele",
        name = "Dejan Zele Pejchev",
        email = "pejcev.dejan@gmail.com",
        url = url("https://github.com/dejanzele/")
      ),
      Developer(
        id = "EnricoMi",
        name = "Enrico Minack",
        email = "github@enrico.minack.dev",
        url = url("https://github.com/EnricoMi/")
      ),
      Developer(
        id = "GeorgeJahad",
        name = "George Jahad",
        email = "github@blackbirdsystems.net",
        url = url("https://github.com/GeorgeJahad/")
      ),
      Developer(
        id = "richscott",
        name = "Rich Scott",
        email = "rich@gr-oss.io",
        url = url("https://github.com/richscott/")
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
