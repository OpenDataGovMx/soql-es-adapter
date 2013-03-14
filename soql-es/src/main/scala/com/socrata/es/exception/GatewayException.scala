package com.socrata.es.exception

class GatewayException(message: String) extends Exception(message)

case class GatewayNotFound(message: String) extends GatewayException(message)

object GatewayException {
  def apply(statusCode: Int, message: String): GatewayException = {
    statusCode match {
      case 404 => GatewayNotFound(message)
      case _ => new GatewayException(message)
    }
  }
}
