/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib.amqp

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpec }

abstract class AmqpSpec extends WordSpec with Matchers with BeforeAndAfterAll with ScalaFutures {

  implicit val system = ActorSystem(this.getClass.getSimpleName)
  implicit val materializer = ActorMaterializer()

  override protected def afterAll(): Unit = {
    system.terminate()
  }
}
