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
    jodaConvert,
    jodaTime,
    opencsv,
    rojomaJson,
    simpleArm,
    socrataUtil
  )
}


