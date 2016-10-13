lazy val ftp = (project in file(".")).
  enablePlugins(AutomateHeaderPlugin)

name := "akka-stream-contrib-ftp"

libraryDependencies ++= Seq(
  "commons-net" % "commons-net" % "3.5",
  "com.jcraft" % "jsch" % "0.1.54",
  "org.apache.ftpserver" % "ftpserver-core" % "1.0.6" % "test",
  "org.slf4j" % "slf4j-log4j12" % "1.7.21" % "test"
)
