import sbt.*

object Lib {
  object Version {
    val scala      = "3.3.0-RC4"
    val service    = "0.1.0-SNAPSHOT"
    val config     = "1.4.2"
    val catsEffect = "3.5.0-RC3"
    val circe      = "0.14.5"
    val fs2        = "3.7.0-RC4"
    val decline    = "2.4.1"
  }

  val config: List[ModuleID] = List(
    "com.typesafe" % "config" % Version.config
  )

  val circe: List[ModuleID] = List(
    "io.circe" %% "circe-core"    % Version.circe,
    "io.circe" %% "circe-parser"  % Version.circe,
    "io.circe" %% "circe-literal"  % Version.circe
  )

  val catsEffect: List[ModuleID] = List(
    "org.typelevel" %% "cats-effect" % Version.catsEffect
  )

  val fs2: List[ModuleID] = List(
    "co.fs2" %% "fs2-core" % Version.fs2,
    "co.fs2" %% "fs2-io"   % Version.fs2,
  )

  val decline: List[ModuleID] = List(
    "com.monovore" %% "decline-effect" % Version.decline
  )

}
