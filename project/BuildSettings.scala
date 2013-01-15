import com.socrata.socratasbt.SocrataSbt.SocrataUtil.{Is210, Is29}
import sbt._
import Keys._

import com.socrata.socratasbt.SocrataSbt._
import sbtassembly.Plugin._
import AssemblyKeys._
import sbtassembly.AssemblyUtils.sourceOfFileForMerge

object BuildSettings {
  val buildSettings: Seq[Setting[_]] = Defaults.defaultSettings ++ socrataBuildSettings ++ Seq(
    scalaVersion := "2.10.0",
    scalacOptions ++= Seq("-language:implicitConversions")
  )

  def projectSettings(assembly: Boolean = false) = buildSettings ++ socrataProjectSettings(assembly) ++
    (if(assembly) Seq(mergeStrategy in AssemblyKeys.assembly ~= mergeStrategies _) else Seq())

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
