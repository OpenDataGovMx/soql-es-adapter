import sbt._
import Keys._
import Dependencies._

object StoreES {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly=true) ++ Seq(
    libraryDependencies <++= scalaVersion(libraries(_))
  )

  def libraries(implicit scalaVersion: String) = Seq(
    "com.typesafe" % "config" % "1.0.0",
    "org.scalaz" %% "scalaz-core" % "7.0.0-M7",
    "org.scalaz" %% "scalaz-effect" % "7.0.0-M7",
    dataCoordinator
  )
}


