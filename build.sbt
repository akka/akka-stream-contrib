lazy val root = (project in file(".")).
  aggregate(contrib, xmlparser).
  enablePlugins(GitVersioning)

lazy val contrib = project
lazy val xmlparser = project

git.useGitDescribe := true
publishArtifact := false
