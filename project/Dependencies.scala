import sbt._

object Dependencies {
  object versions {
    val asyncHttpClient = "1.7.5"
    val commonsCli = "1.2"
    val jodaConvert = "1.2"
    val jodaTime = "2.1"
    val opencsv = "2.1"
    val rojomaJson = "2.0.0"
    val scalaCheck_28 = "1.8"
    val scalaCheck_29 = "1.10.0"
    val scalaTest = "1.8"
    val simpleArm = "1.1.10"
    val slf4j = "1.6.6"
    val socrataCoreMisc = "1.2.4"
    val socrataUtils = "0.5.0"
    val soqlParser = "0.0.5"
  }

  val asyncHttpClient = "com.ning" % "async-http-client" % versions.asyncHttpClient

  val commonsCli = "commons-cli" % "commons-cli" % versions.commonsCli

  val jodaConvert = "org.joda" % "joda-convert" % versions.jodaConvert

  val jodaTime = "joda-time" % "joda-time" % versions.jodaTime

  val opencsv = "net.sf.opencsv" % "opencsv" % versions.opencsv

  val rojomaJson = "com.rojoma" %% "rojoma-json" % versions.rojomaJson

  val scalaTest = "org.scalatest" %% "scalatest" % versions.scalaTest

  val simpleArm = "com.rojoma" %% "simple-arm" % versions.simpleArm

  val slf4j = "org.slf4j" % "slf4j-api" % versions.slf4j

  val slf4jSimple = "org.slf4j" % "slf4j-simple" % versions.slf4j

  val socrataBlistDatatypes = "com.socrata" %% "blist-datatypes" % versions.socrataCoreMisc

  val socrataBlistMisc = "com.socrata" %% "blist-misc" % versions.socrataCoreMisc

  val socrataCoreMisc = "com.socrata" %% "core-misc" % versions.socrataCoreMisc

  val socrataUtil = "com.socrata" %% "socrata-utils" % versions.socrataUtils

  val soqlParser = "com.socrata" %% "soql-parser" % versions.soqlParser


  def scalaCheck(implicit scalaVersion: String) = ScalaVersion.v match {
    case Scala28 => "org.scalacheck" % "scalacheck_2.8.1" % versions.scalaCheck_28
    case Scala29 => "org.scalacheck" %% "scalacheck" % versions.scalaCheck_29
  }
}
