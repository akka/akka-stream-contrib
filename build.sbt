lazy val root = (project in file(".")).
  aggregate(contrib, mqtt, amqp, xmlparser, cassandra, ftp).
  enablePlugins(GitVersioning)

lazy val contrib = project
lazy val mqtt = project
lazy val amqp = project
lazy val xmlparser = project
lazy val cassandra = project
lazy val ftp = project

git.useGitDescribe := true
publishArtifact := false
