import sbt._
import Keys._

object Build extends sbt.Build {
  lazy val build = Project(
    "soql-es-adapter",
    file("."),
    settings = BuildSettings.buildSettings //++ BuildSettings.sonatypeSettings
  ) aggregate (allOtherProjects: _*) dependsOn(soqlES, importES, storeES)

  private def allOtherProjects =
    for {
      method <- getClass.getDeclaredMethods.toSeq
      if method.getParameterTypes.isEmpty && classOf[Project].isAssignableFrom(method.getReturnType) && method.getName != "build"
    } yield method.invoke(this).asInstanceOf[Project] : ProjectReference

  private def p(name: String, settings: { def settings: Seq[Setting[_]] }, dependencies: ClasspathDep[ProjectReference]*) =
    Project(name, file(name), settings = settings.settings) dependsOn(dependencies: _*)

  lazy val soqlES = p("soql-es", SoqlES)
  lazy val importES = p("import-es", ImportES) dependsOn(soqlES)
  lazy val storeES = p("store-es", StoreES) dependsOn(soqlES)
}
