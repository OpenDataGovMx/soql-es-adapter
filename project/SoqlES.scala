import com.socrata.socratasbt.SocrataSbt.SocrataSbtKeys._
import sbt._
import Keys._

import Dependencies._

object SoqlES {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    libraryDependencies <++= (scalaVersion, slf4jVersion) { (scalaVersion, slf4jVersion) =>
      Seq(
        asyncHttpClient,
        jodaConvert,
        jodaTime,
        rojomaJson,
        socrataUtil,
        soqlStdlib,
        typesafeConfig,
        coordinatorlib,
        coordinatorlibSoql,
        slf4j % slf4jVersion
      )
    }
  )
}


