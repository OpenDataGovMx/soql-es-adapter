import com.socrata.socratasbt.SocrataSbt.SocrataSbtKeys._
import sbt._
import Keys._
import Dependencies._

object SoqlServerES {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    libraryDependencies <++= (scalaVersion, slf4jVersion) { (scalaVersion, slf4jVersion) =>
      Seq(
        socrataHttpJetty,
        socrataThirdPartyUtil
      )
    }
  )
}


