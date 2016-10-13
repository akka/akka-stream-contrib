/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib.ftp

import java.io.{ File, FileOutputStream }
import java.nio.file.{ Files, Paths }
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import org.apache.ftpserver.{ ConnectionConfigFactory, FtpServer, FtpServerFactory }
import org.apache.ftpserver.filesystem.nativefs.NativeFileSystemFactory
import org.apache.ftpserver.listener.ListenerFactory
import org.apache.ftpserver.usermanager.{ ClearTextPasswordEncryptor, PropertiesUserManagerFactory }
import org.apache.mina.util.AvailablePortFinder
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpec }
import scala.concurrent.Await
import scala.util.control.NonFatal
import scala.concurrent.duration.DurationInt

trait BaseFtpSpec extends WordSpec with Matchers with BeforeAndAfterAll with ScalaFutures {

  final val FtpRootDir = "./target/res/home"
  final val UsersFile = new File(getClass.getClassLoader.getResource("users.properties").getFile)
  final val DefaultListener = "default"
  final val BasePort = 21000

  protected var ftpServer: FtpServer = null
  protected var port: Option[Int] = None

  protected implicit val system = {
    def config = ConfigFactory.parseString(s"akka.stream.materializer.auto-fusing=true")
      .withFallback(ConfigFactory.load())
    ActorSystem("default", config)
  }

  protected implicit val mat = ActorMaterializer()

  override protected def beforeAll() = {
    super.beforeAll()
    deleteDirectory(FtpRootDir)
    Files.createDirectories(Paths.get(FtpRootDir))
    port = Some(AvailablePortFinder.getNextAvailable(BasePort))
    val factory = createFtpServerFactory(port)
    if (factory != null) {
      ftpServer = factory.createServer()
      if (ftpServer != null) {
        ftpServer.start()
      }
    }
  }

  override protected def afterAll() = {
    if (ftpServer != null) {
      try {
        ftpServer.stop()
        ftpServer = null
      } catch {
        case NonFatal(t) => // ignore
      }
    }
    Await.ready(system.terminate(), 42.seconds)
    deleteDirectory(FtpRootDir)
    super.afterAll()
  }

  protected[ftp] def generateFiles(numFiles: Int = 30) =
    (1 to numFiles).foreach { i =>
      putFileOnFtp(s"$FtpRootDir/sample_$i")
    }

  private[this] def createFtpServerFactory(port: Option[Int]) = {
    assert(UsersFile.exists())

    val fsf = new NativeFileSystemFactory
    fsf.setCreateHome(true)

    val pumf = new PropertiesUserManagerFactory
    pumf.setAdminName("admin")
    pumf.setPasswordEncryptor(new ClearTextPasswordEncryptor)
    pumf.setFile(UsersFile)
    val userMgr = pumf.createUserManager()

    val factory = new ListenerFactory
    factory.setPort(port.getOrElse(AvailablePortFinder.getNextAvailable(BasePort)))

    val serverFactory = new FtpServerFactory
    serverFactory.setUserManager(userMgr)
    serverFactory.setFileSystem(fsf)
    serverFactory.setConnectionConfig(new ConnectionConfigFactory().createConnectionConfig())
    serverFactory.addListener(DefaultListener, factory.createListener())

    serverFactory
  }

  private[this] def deleteDirectory(file: String): Unit =
    deleteDirectory(new File(file))

  private[this] def deleteDirectory(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteDirectory)
    file.delete()
  }

  private[this] def putFileOnFtp(path: String) = {
    val fos = new FileOutputStream(path)
    fos.write(loremIpsum)
    fos.close()
  }

  private[this] def loremIpsum =
    """|Lorem ipsum dolor sit amet, consectetur adipiscing elit. Praesent auctor imperdiet
       |velit, eu dapibus nisl dapibus vitae. Sed quam lacus, fringilla posuere ligula at,
       |aliquet laoreet nulla. Aliquam id fermentum justo. Aliquam et massa consequat,
       |pellentesque dolor nec, gravida libero. Phasellus elit eros, finibus eget
       |sollicitudin ac, consectetur sed ante. Etiam ornare lacus blandit nisi gravida
       |accumsan. Sed in lorem arcu. Vivamus et eleifend ligula. Maecenas ut commodo ante.
       |Suspendisse sit amet placerat arcu, porttitor sagittis velit. Quisque gravida mi a
       |porttitor ornare. Cras lorem nisl, sollicitudin vitae odio at, vehicula maximus
       |mauris. Sed ac purus ac turpis pellentesque cursus ac eget est. Pellentesque
       |habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas.
       |""".stripMargin.toCharArray.map(_.toByte)
}
