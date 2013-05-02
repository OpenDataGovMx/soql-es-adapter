package com.socrata.es.server.config

import com.typesafe.config.Config

class SecondaryConfig(config: Config) {
  val url = config.getString("url")
}
