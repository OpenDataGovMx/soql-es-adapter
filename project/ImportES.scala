import com.socrata.socratasbt.SocrataSbt._
import sbt._
import Keys._

import Dependencies._

object ImportES {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly=true) ++ Seq(
    libraryDependencies <++= scalaVersion(libraries(_))
  )

  def libraries(implicit scalaVersion: String) = Seq(
    asyncHttpClient,
    commonsCli,
    opencsv,
    rojomaJson,
    simpleArm,
    slf4jSimple,
    socrataBlistMisc,
    socrataCoreMisc,
    socrataUtil,
    soqlParser
  )
}


