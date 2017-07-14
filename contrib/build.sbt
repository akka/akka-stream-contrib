lazy val contrib = (project in file(".")).
  enablePlugins(AutomateHeaderPlugin)

name := "akka-stream-contrib"

// By default scalatest futures time out in 150 ms, dilate that to 600ms.
// This should not impact the total test time as we don't expect to hit this
// timeout, and indeed it doesn't appear to.
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-F", "4")

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream-testkit" % Common.AkkaVersion % "provided",
  "junit"             %  "junit"               % "4.12" % Test, // Common Public License 1.0
  "com.novocode"      %  "junit-interface"     % "0.11" % Test, // BSD-like
  "com.google.jimfs"  %  "jimfs"               % "1.1"  % Test  // ApacheV2
)
