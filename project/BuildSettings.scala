import sbt._
import Keys._

import sbtassembly.Plugin._
import AssemblyKeys._

import Dependencies._

object BuildSettings {
  val buildSettings: Seq[Setting[_]] = Defaults.defaultSettings ++ Seq(
    organization := "com.socrata",
    version := "0.0.1-SNAPSHOT",
    scalaVersion := "2.9.2",
    resolvers := Seq(
      "socrata maven" at "https://repo.socrata.com/artifactory/libs-release",
      Resolver.url("socrata ivy", new URL("https://repo.socrata.com/artifactory/ivy-libs-release"))(Resolver.ivyStylePatterns)
    ),
    externalResolvers <<= resolvers map { rs =>
      Resolver.withDefaultResolvers(rs, mavenCentral = false)
    }
  )

  def commonProjectSettings(assembly: Boolean = false): Seq[Setting[_]] =
    buildSettings ++
    (if(assembly) commonAssemblySettings else Seq.empty) ++
    Seq(
      scalacOptions <++= scalaVersion map compilerFlags,
      testOptions in Test ++= Seq(
        Tests.Argument(TestFrameworks.ScalaTest, "-oD")
      ),
      libraryDependencies <++= scalaVersion(commonLibraries(_))
    )

  def compilerFlags(sv: String) = ScalaVersion.v(sv) match {
    case Scala28 => Seq("-encoding", "UTF-8", "-g", "-unchecked", "-deprecation")
    case Scala29 => Seq("-encoding", "UTF-8", "-g:vars", "-unchecked", "-deprecation")
  }

  def commonLibraries(implicit scalaVersion: String) = Seq(
    slf4j,
    scalaTest % "test",
    slf4jSimple % "test"
  )

  def commonAssemblySettings: Seq[Setting[_]] = assemblySettings ++ Seq(
    test in assembly := {}
  )
}
