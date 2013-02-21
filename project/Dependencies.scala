import sbt._

object Dependencies {
  object versions {
    val asyncHttpClient = "1.7.5"
    val commonsCli = "1.2"
    val jodaConvert = "1.2"
    val jodaTime = "2.1"
    val opencsv = "2.1"
    val rojomaJson = "2.2.0"
    val scalaCheck_28 = "1.8"
    val scalaCheck_29 = "1.10.0"
    val scalaTest = "1.8"
    val simpleArm = "1.1.10"
    val socrataUtils = "0.6.0"
    val soqlStdlib = "0.0.11"
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

  val socrataUtil = "com.socrata" %% "socrata-utils" % versions.socrataUtils

  val soqlStdlib = "com.socrata" %% "soql-stdlib" % versions.soqlStdlib

  val dataCoordinator = "com.socrata" %% "coordinator" % versions.dataCoordinator
}
