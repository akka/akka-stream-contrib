/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.cluster.http

import akka.actor.AddressFromURIString
import akka.cluster.{Cluster, Member}
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.RouteResult
import akka.stream.ActorMaterializer
import spray.json._

import scala.concurrent.Future

final case class ClusterUnreachableMember(node: String, observedBy: Seq[String])
final case class ClusterMember(node: String, nodeUid: String, status: String, roles: Set[String])
final case class ClusterMembers(selfNode: String, members: Set[ClusterMember], unreachable: Seq[ClusterUnreachableMember])
final case class ClusterHttpApiMessage(message: String)

object ClusterHttpApiOperation extends Enumeration {
  val DOWN, LEAVE, JOIN = Value

  def find(name: String) = try {
    Some(withName(name))
  } catch {
    case e: NoSuchElementException =>
      None
  }
}

trait ClusterHttpApiJsonProtocol extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val clusterUnreachableMemberFormat = jsonFormat2(ClusterUnreachableMember)
  implicit val clusterMemberFormat = jsonFormat4(ClusterMember)
  implicit val clusterMembersFormat = jsonFormat3(ClusterMembers)
  implicit val clusterMemberMessageFormat = jsonFormat1(ClusterHttpApiMessage)
}

trait ClusterHttpApiHelper extends ClusterHttpApiJsonProtocol {
  def memberToClusterMember(m: Member): ClusterMember = {
    ClusterMember(s"${m.uniqueAddress.address}", s"${m.uniqueAddress.uid}", s"${m.status}", m.roles)
  }
}

object ClusterHttpRoute extends ClusterHttpApiHelper {

  private def routeGetMembers(cluster: Cluster) = {
    get {
      complete {
        val members = cluster.readView.state.members.map(memberToClusterMember)

        val unreachable = cluster.readView.reachability.observersGroupedByUnreachable.toSeq.sortBy(_._1).map {
          case (subject, observers) ⇒
            ClusterUnreachableMember(s"${subject.address}", observers.toSeq.sorted.map(m ⇒ s"${m.address}"))
        }

        ClusterMembers(s"${cluster.readView.selfAddress}", members, unreachable)
      }
    }
  }

  private def routePostMembers(cluster: Cluster) = {
    post {
      formField('address) { address ⇒
        complete {
          cluster.join(AddressFromURIString(address))
          ClusterHttpApiMessage("Operation executed")
        }
      }
    }
  }

  private def routeGetMember(cluster: Cluster, member: String) =
    get {
      complete {
        cluster.readView.members.find(m ⇒ s"${m.uniqueAddress.address}" == member).map(memberToClusterMember)
      }
    }

  private def routeDeleteMember(cluster: Cluster, member: String) =
    delete {
      cluster.readView.members.find(m ⇒ s"${m.uniqueAddress.address}" == member) match {
        case Some(m) =>
          cluster.leave(m.uniqueAddress.address)
          complete(ClusterHttpApiMessage("Operation executed"))
        case None =>
          complete(StatusCodes.NotFound -> ClusterHttpApiMessage("Member not found"))
      }
    }

  private def routePutMember(cluster: Cluster, member: String) =
    put {
      formField('operation) { operation =>
        cluster.readView.members.find(m ⇒ s"${m.uniqueAddress.address}" == member) match {
          case Some(m) =>
            ClusterHttpApiOperation.find(operation.toUpperCase) match {
              case Some(ClusterHttpApiOperation.DOWN) =>
                cluster.down(m.uniqueAddress.address)
                complete(ClusterHttpApiMessage("Operation executed"))
              case Some(ClusterHttpApiOperation.LEAVE) =>
                cluster.leave(m.uniqueAddress.address)
                complete(ClusterHttpApiMessage("Operation executed"))
              case _ =>
                complete(StatusCodes.NotFound -> ClusterHttpApiMessage("Operation not supported"))
            }
          case None =>
            complete(StatusCodes.NotFound -> ClusterHttpApiMessage("Member not found"))
        }
      }
    }

  def apply(cluster: Cluster) =
    pathPrefix("members") {
      pathEndOrSingleSlash {
        routeGetMembers(cluster) ~ routePostMembers(cluster)
      } ~
      path(Remaining) { member ⇒
        routeGetMember(cluster, member) ~ routeDeleteMember(cluster, member) ~ routePutMember(cluster, member)
      }
    }
}

/**
  * Class to instantiate an [[akka.cluster.http.ClusterHttpApi]] to
  * provide an HTTP management interface for [[akka.cluster.Cluster]].
  */
class ClusterHttpApi(cluster: Cluster) {
  private val settings = new ClusterHttpApiSettings(cluster.system.settings.config)
  private implicit val system = cluster.system
  private implicit val materializer = ActorMaterializer()
  import system._

  private var bindingFuture : Future[Http.ServerBinding] = _

  def start() = {
    val route = RouteResult.route2HandlerFlow(ClusterHttpRoute(cluster))
    bindingFuture = Http().bindAndHandle(route, settings.httpApiHostname, settings.httpApiPort)
  }

  def stop() = {
    bindingFuture.flatMap(_.unbind())
  }
}
