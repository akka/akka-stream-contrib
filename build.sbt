lazy val `akka-stream-contrib` = (project in file(".")).
  aggregate(contrib)

lazy val contrib = project

publishArtifact := false
