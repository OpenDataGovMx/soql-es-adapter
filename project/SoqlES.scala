import com.socrata.socratasbt.SocrataSbt._
import sbt._
import Keys._

import Dependencies._

object SoqlES {
  lazy val settings: Seq[Setting[_]] = BuildSettings.buildSettings ++ socrataProjectSettings() ++ Seq(
    libraryDependencies <++= scalaVersion(libraries(_))
  )

  def libraries(implicit scalaVersion: String) = Seq(
    asyncHttpClient,
    rojomaJson,
    slf4jSimple,
    socrataBlistMisc,
    socrataCoreMisc,
    socrataUtil,
    soqlParser
  )
}


