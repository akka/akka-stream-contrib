lazy val contrib = (project in file(".")).
  enablePlugins(AutomateHeaderPlugin)

name := "akka-stream-contrib"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream-testkit" % Common.AkkaVersion % "provided",
  "junit"             %  "junit"               % "4.12" % Test, // Common Public License 1.0
  "com.novocode"      %  "junit-interface"     % "0.11" % Test, // BSD-like
  "com.google.jimfs"  %  "jimfs"               % "1.1"  % Test  // ApacheV2
)
