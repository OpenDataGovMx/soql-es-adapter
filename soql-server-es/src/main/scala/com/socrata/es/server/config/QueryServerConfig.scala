package com.socrata.es.server.config

import com.typesafe.config.Config

class QueryServerConfig(config: Config) {

  val log4j = config.getConfig("log4j")
  val secondary = new SecondaryConfig(config.getConfig("secondary-es"))
  val port = config.getInt("port")
}
