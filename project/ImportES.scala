import com.socrata.socratasbt.SocrataSbt._
import sbt._
import Keys._

import Dependencies._

object ImportES {
  lazy val settings: Seq[Setting[_]] = BuildSettings.buildSettings ++ socrataProjectSettings(assembly=true) ++ Seq(
    libraryDependencies <++= scalaVersion(libraries(_))
  )

  def libraries(implicit scalaVersion: String) = Seq(
    asyncHttpClient,
    commonsCli,
    opencsv,
    rojomaJson,
    slf4jSimple,
    socrataBlistMisc,
    socrataCoreMisc,
    socrataUtil,
    soqlParser
  )
}


