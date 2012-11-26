import sbt._
import Keys._

import sbtassembly.AssemblyUtils._
import sbtassembly.Plugin._
import AssemblyKeys._

import Dependencies._
import scala.Right
import scala.Some

object BuildSettings {
  val buildSettings: Seq[Setting[_]] = Defaults.defaultSettings ++ socrataBuildSettings ++ Seq(
    scalaVersion := "2.9.2"
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
    test in assembly := {},
    mergeStrategy in assembly ~= mergeStrategies _
  )

  def mergeStrategies(originalMergeStrategiesFunc: String => MergeStrategy)(name: String): MergeStrategy = name match {
    case "overview.html" => MergeStrategy.discard
    case n => originalMergeStrategiesFunc(n) match {
      case MergeStrategy.rename => MergeStrategy.rename
      case other => wrappedMergeStrategy(other)
    }
  }

  private val wrappedStrategies = new scala.collection.mutable.HashMap[MergeStrategy, MergeStrategy]
  def wrappedMergeStrategy(originalStrat: MergeStrategy): MergeStrategy = synchronized {
    wrappedStrategies.get(originalStrat) match {
      case Some(wrapped) => wrapped
      case None =>
        val newStrat = new MergeStrategy {
          def apply(args: (File, String, Seq[File])): Either[String, Seq[(File, String)]] = {
            val (tmpDir, name, files) = args
            val sources = files.map(sourceOfFileForMerge(tmpDir, _))
            if(sources.forall(_._4)) { // all came from jars
            def pick(jarName: String): Either[Nothing, Seq[(File, String)]] = {
              for((file, source) <- files.zip(sources)) if(source._1.name == jarName) return Right(Seq(file -> name))
              sys.error("Got a typo in your pick line, mate!")
            }
              sources.map(_._1.name).sorted match {
                  case Seq("commons-beanutils-1.7.0.jar", "commons-collections-3.2.jar") => pick("commons-collections-3.2.jar")
                  case Seq("commons-logging-1.0.4.jar", "jcl-over-slf4j-1.7.1.jar") => pick("commons-logging-1.0.4.jar")
                case _ => originalStrat(args)
              }
            } else {
              originalStrat(args)
            }
          }
          def name = "resolveStupidConflicts(" + originalStrat.name + ")"
          override def notifyThreshold = math.min(2, originalStrat.notifyThreshold)
        }
        wrappedStrategies(originalStrat) = newStrat
        newStrat
    }
  }
}
