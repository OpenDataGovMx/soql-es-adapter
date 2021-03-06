import sbt._

object Dependencies {
  object versions {
    val asyncHttpClient = "1.7.5"
    val commonsCli = "1.2"
    val jodaConvert = "1.2"
    val jodaTime = "2.1"
    val opencsv = "2.1"
    val rojomaJson = "[2.3.0,3.0.0)"
    val scalaCheck_28 = "1.8"
    val scalaCheck_29 = "1.10.0"
    val scalaTest = "1.8"
    val simpleArm = "1.1.10"
    val socrataUtils = "[0.6.0,0.7.0)"
    val socrataThirdPartyUtil = "[2.0.0,3.0.0)"
    val socrataHttpJetty = "[1.3.0,1.4.0)"
    val soqlStdlib = "0.0.16-SNAPSHOT"
    val typesafeConfig = "1.0.0"
    val dataCoordinator = "0.0.1-SNAPSHOT"
  }

  val asyncHttpClient = "com.ning" % "async-http-client" % versions.asyncHttpClient

  val commonsCli = "commons-cli" % "commons-cli" % versions.commonsCli

  val jodaConvert = "org.joda" % "joda-convert" % versions.jodaConvert

  val jodaTime = "joda-time" % "joda-time" % versions.jodaTime

  val opencsv = "net.sf.opencsv" % "opencsv" % versions.opencsv

  val rojomaJson = "com.rojoma" %% "rojoma-json" % versions.rojomaJson

  val scalaTest = "org.scalatest" %% "scalatest" % versions.scalaTest

  val simpleArm = "com.rojoma" %% "simple-arm" % versions.simpleArm

  val socrataHttpJetty = "com.socrata" %% "socrata-http-jetty" % versions.socrataHttpJetty

  val socrataUtil = "com.socrata" %% "socrata-utils" % versions.socrataUtils

  val socrataThirdPartyUtil = "com.socrata" %% "socrata-thirdparty-utils" % versions.socrataThirdPartyUtil

  val soqlStdlib = "com.socrata" %% "soql-stdlib" % versions.soqlStdlib

  val typesafeConfig = "com.typesafe" % "config" % versions.typesafeConfig

  val coordinator = "com.socrata" %% "coordinator" % versions.dataCoordinator // % "provided"

  val coordinatorlib = "com.socrata" %% "coordinatorlib" % versions.dataCoordinator // % "provided"

  val coordinatorlibSoql = "com.socrata" %% "coordinatorlib-soql" % versions.dataCoordinator // % "provided"

  val slf4j = "org.slf4j" % "slf4j-log4j12"
}
