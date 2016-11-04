lazy val `cluster-http` = (project in file(".")).
  enablePlugins(AutomateHeaderPlugin)

name := "akka-cluster-http"

libraryDependencies ++= Seq(
  "com.typesafe.akka"      %% "akka-cluster"                        % Common.AkkaVersion,
  "com.typesafe.akka"      %% "akka-http-experimental"              % Common.AkkaVersion,
  "com.typesafe.akka"      %% "akka-http-spray-json-experimental"   % Common.AkkaVersion,
  "com.typesafe.akka"      %% "akka-http-testkit"                   % Common.AkkaVersion   % "test",
  "org.mockito"             % "mockito-core"                        % "2.2.6"              % "test"
)
