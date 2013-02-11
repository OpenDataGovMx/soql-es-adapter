import sbt._
import Keys._

import Dependencies._

object SoqlES {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    libraryDependencies <++= scalaVersion(libraries(_))
  )

  def libraries(implicit scalaVersion: String) = Seq(
    asyncHttpClient,
    jodaConvert,
    jodaTime,
    rojomaJson,
    socrataUtil,
    soqlStdlib
  )
}


