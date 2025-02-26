addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.7")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.13"

libraryDependencies ++= Seq(
  "com.google.protobuf" % "protobuf-java" % "3.13.0" % "protobuf"
)
