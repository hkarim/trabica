
lazy val commonScalaOptions = List(
  "-deprecation",
  "-encoding",
  "utf-8",
  "-explaintypes",
  "-feature",
  "-no-indent",
  "-Xfatal-warnings",
  "-Wunused:all",
)

lazy val fullScalaOptions =
  commonScalaOptions ++ List(
    "-Wvalue-discard",
  )

lazy val commonSettings = List(
  organization := s"io.hk",
  scalaVersion := Lib.Version.scala,
  version := Lib.Version.service,
  Compile / packageDoc / mappings := Seq.empty,
)

lazy val trabica = project
  .in(file("."))
  .settings(commonSettings)
  .settings(
    name := "trabica",
  )
  .aggregate(`lib-trabica-rpc`)
  .aggregate(`lib-trabica-api`)
  .aggregate(`node-template`)

lazy val `lib-trabica-rpc` = project
  .in(file("mod/lib-trabica-rpc"))
  .enablePlugins(Fs2Grpc)
  .settings(commonSettings)
  .settings(scalacOptions ++= commonScalaOptions)
  .settings(PB.protocVersion := Lib.Version.protocVersion)
  .settings(
    name := "lib-trabica-rpc",
  )
  .settings(
    libraryDependencies ++= Lib.grpc
  )

lazy val `lib-trabica-api` = project
  .in(file("mod/lib-trabica-api"))
  .settings(commonSettings)
  .settings(scalacOptions ++= fullScalaOptions)
  .settings(
    name := "lib-trabica-api",
  )
  .settings(
    libraryDependencies ++=
      Lib.config ++
        Lib.catsEffect ++
        Lib.fs2 ++
        Lib.decline ++
        Lib.scribe
  )
  .dependsOn(`lib-trabica-rpc`)

lazy val `node-template` = project
  .in(file("mod/node-template"))
  .settings(
    name := "node-template"
  )
  .enablePlugins(JavaAppPackaging)
  .enablePlugins(GraalVMNativeImagePlugin)
  .settings(commonSettings)
  .settings(scalacOptions ++= fullScalaOptions)
  .settings(
    name := "node-template",
  )
  .settings(
    graalVMNativeImageOptions ++= List(
      "-H:IncludeResources=(reference|application).conf$",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.grpc.netty.*",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.bootstrap.*",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.buffer.*",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.channel.*",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.channel.unix.Errors",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.channel.unix.IovArray",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.channel.unix.Limits",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.channel.epoll.*",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.channel.epoll.Epoll",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.channel.epoll.Native",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.channel.epoll.EpollEventLoop",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.channel.epoll.EpollEventArray",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.handler.*",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.handler.ssl.*",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.handler.ssl.OpenSsl",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.handler.ssl.OpenSslPrivateKeyMethod",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.handler.ssl.ReferenceCountedOpenSslEngine",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.handler.ssl.OpenSslAsyncPrivateKeyMethod",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.internal.*",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.internal.tcnative.AsyncSSLPrivateKeyMethod",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.internal.tcnative.CertificateVerifier",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.internal.tcnative.SSL",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.internal.tcnative.SSLPrivateKeyMethod",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.resolver.*",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.util.*",
      "--no-fallback",
    )
  )
  .settings(
    libraryDependencies ++= Lib.decline,
  )
  .dependsOn(`lib-trabica-api`)
  .settings(List(Compile / mainClass := Some("trabica.node.Service")))




