/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.contrib

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

trait BaseStreamSpec extends WordSpec with Matchers with BeforeAndAfterAll {

  protected implicit val system = {
    def systemConfig =
      config.withFallback(ConfigFactory.load())
    ActorSystem("default", systemConfig)
  }

  protected implicit val mat = ActorMaterializer()

  override protected def afterAll() = {
    Await.ready(system.terminate(), 42.seconds)
    super.afterAll()
  }

  protected def config: Config = ConfigFactory.empty()
}
