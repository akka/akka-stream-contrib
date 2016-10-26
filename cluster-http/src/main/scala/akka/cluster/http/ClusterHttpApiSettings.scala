/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.cluster.http

import com.typesafe.config.Config

final class ClusterHttpApiSettings(val config: Config) {
  private val cc = config.getConfig("akka.cluster.http-api")
  val httpApiPort = cc.getInt("port")
  val httpApiHostname = cc.getString("hostname")
}
