lazy val commonSettings = List(
  scalaVersion := Lib.Version.scala,
  version      := Lib.Version.service,
  scalacOptions ++= List(
    "-deprecation",
    "-encoding",
    "utf-8",
    "-explaintypes",
    "-feature",
    "-no-indent",
    "-Xfatal-warnings",
    "-Wunused:all",
    //"-Wvalue-discard",
  ),
  Compile / packageDoc / mappings := List.empty,
)

lazy val trabica = project
  .in(file("."))
  .settings(commonSettings)
  .settings(
    name := "trabica",
  )
  .aggregate(`lib-trabica-api`)
  .aggregate(`node-template`)

lazy val `lib-trabica-api` = project
  .in(file("mod/lib-trabica-api"))
  .enablePlugins(Fs2Grpc)
  .settings(commonSettings)
  .settings(PB.protocVersion := Lib.Version.protocVersion)
  .settings(
    name := "lib-trabica-api",
  )
  .settings(
    libraryDependencies ++=
      Lib.config ++
        Lib.catsEffect ++
        Lib.fs2 ++
        Lib.grpc ++
        Lib.decline ++
        Lib.scribe
  )

lazy val `node-template` = project
  .in(file("mod/node-template"))
  .enablePlugins(JavaAppPackaging)
  .enablePlugins(GraalVMNativeImagePlugin)
  .settings(commonSettings)
  .settings(
    name := "node-template",
  )
  .settings(
    graalVMNativeImageOptions ++= List(
      "-H:IncludeResources=(reference|application).conf$",
      "--no-fallback",
    )
  )
  .settings(
    libraryDependencies ++= Lib.decline,
  )
  .dependsOn(`lib-trabica-api`)
  .settings(List(Compile / mainClass := Some("trabica.node.Service")))




