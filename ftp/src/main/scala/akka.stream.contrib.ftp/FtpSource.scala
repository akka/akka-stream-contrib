/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib.ftp

import java.net.InetAddress
import akka.stream.{ Attributes, Outlet, SourceShape }
import akka.stream.Attributes.{ InputBuffer, name }
import akka.stream.contrib.ftp.FtpConnectionSettings.{ BasicFtpConnectionSettings, DefaultFtpConnectionSettings, DefaultFtpPort }
import akka.stream.contrib.ftp.FtpCredentials.{ AnonFtpCredentials, NonAnonFtpCredentials }
import akka.stream.scaladsl.Source
import akka.stream.stage.{ GraphStageLogic, GraphStageWithMaterializedValue, OutHandler }
import org.apache.commons.net.ftp.{ FTPClient, FTPListParseEngine }
import scala.concurrent.{ Future, Promise }
import scala.util.control.NonFatal

object FtpSource {

  final val SourceName = "ftpSource"

  def apply(): Source[FtpFile, Future[Long]] = apply(DefaultFtpConnectionSettings)

  def apply(hostname: String): Source[FtpFile, Future[Long]] = apply(hostname, DefaultFtpPort)

  def apply(hostname: String, port: Int): Source[FtpFile, Future[Long]] =
    apply(BasicFtpConnectionSettings(InetAddress.getByName(hostname), port, AnonFtpCredentials))

  def apply(hostname: String, username: String, password: String): Source[FtpFile, Future[Long]] =
    apply(hostname, DefaultFtpPort, username, password)

  def apply(hostname: String, port: Int, username: String, password: String): Source[FtpFile, Future[Long]] =
    apply(BasicFtpConnectionSettings(InetAddress.getByName(hostname), port, NonAnonFtpCredentials(username, password)))

  def apply(connectionSettings: FtpConnectionSettings): Source[FtpFile, Future[Long]] =
    Source.fromGraph(new FtpSource(connectionSettings)).withAttributes(name(SourceName))

}

final class FtpSource private (connectionSettings: FtpConnectionSettings)
  extends GraphStageWithMaterializedValue[SourceShape[FtpFile], Future[Long]] {
  import FtpSource._

  val matValue = Promise[Long]()

  val shape = SourceShape(Outlet[FtpFile](s"$SourceName.out"))

  def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {

    val InputBuffer(initialBuffer, maxBuffer) =
      inheritedAttributes.getAttribute(classOf[InputBuffer], InputBuffer(16, 16))

    val logic = new GraphStageLogic(shape) {
      import shape._

      private var ftpClient: FTPClient = null
      private var ftpListParseEngine: FTPListParseEngine = null
      private var buffer: Vector[FtpFile] = Vector.empty
      private var numFilesTotal: Long = 0L

      override def preStart(): Unit = {
        super.preStart()
        try {
          connect(
            connectionSettings.host,
            connectionSettings.port,
            connectionSettings.credentials.username,
            connectionSettings.credentials.password
          )
          ftpListParseEngine = ftpClient.initiateListParsing()
          fillBuffer(initialBuffer)
        } catch {
          case NonFatal(t) =>
            disconnect()
            matFailure(t)
            failStage(t)
        }
      }

      override def postStop(): Unit = {
        disconnect()
        super.postStop()
      }

      setHandler(out, new OutHandler {
        def onPull(): Unit = {
          fillBuffer(maxBuffer)
          buffer match {
            case Seq() =>
              finalize()
            case head +: Seq() =>
              push(out, head)
              finalize()
            case head +: tail =>
              push(out, head)
              buffer = tail
          }
          def finalize() = try {
            disconnect()
          } finally {
            matSuccess()
            complete(out)
          }
        } // end of onPull

        override def onDownstreamFinish(): Unit = try {
          disconnect()
        } finally {
          matSuccess()
          super.onDownstreamFinish()
        }
      }) // end of handler

      private[this] def connect(host: InetAddress, port: Int, username: String, password: String) = {
        if (ftpClient == null) {
          ftpClient = new FTPClient
        }
        if (!ftpClient.isConnected) {
          ftpClient.connect(host, port)
          ftpClient.login(username, password)
          ftpClient.enterLocalPassiveMode()
        }
      }

      private[this] def disconnect() = {
        if (ftpClient != null) {
          if (ftpClient.isConnected) {
            ftpClient.logout()
            ftpClient.disconnect()
          }
        }
      }

      private[this] def eof = !ftpListParseEngine.hasNext

      private[this] def fillBuffer(size: Int) =
        if (buffer.length < size && !eof) {
          val pageSize = size - buffer.length
          val newPage: Array[FtpFile] = ftpListParseEngine.getNext(pageSize)
          buffer ++= newPage
          numFilesTotal += newPage.length
        }

      private[this] def matSuccess() = matValue.success(numFilesTotal)

      private[this] def matFailure(t: Throwable) = matValue.failure(t)

    } // end of stage logic

    (logic, matValue.future)
  }
}
