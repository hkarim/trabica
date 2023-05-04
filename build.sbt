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
  organization                    := s"io.hk",
  scalaVersion                    := Lib.Version.scala,
  version                         := Lib.Version.service,
  Compile / packageDoc / mappings := Seq.empty,
)

lazy val trabica = project
  .in(file("."))
  .settings(commonSettings)
  .settings(
    name := "trabica",
  )
  .aggregate(`lib-trabica-proto`)
  .aggregate(`lib-trabica-model`)
  .aggregate(`lib-trabica-net`)
  .aggregate(`lib-trabica-rpc`)
  .aggregate(`lib-trabica-store`)
  .aggregate(`lib-trabica-node`)
  .aggregate(`node-template`)

lazy val `lib-trabica-proto` = project
  .in(file("mod/lib-trabica-proto"))
  .enablePlugins(Fs2Grpc)
  .settings(commonSettings)
  .settings(scalacOptions ++= commonScalaOptions)
  .settings(PB.protocVersion := Lib.Version.protocVersion)
  .settings(
    name := "lib-trabica-proto",
  )

lazy val `lib-trabica-model` = project
  .in(file("mod/lib-trabica-model"))
  .settings(commonSettings)
  .settings(scalacOptions ++= fullScalaOptions)
  .settings(
    name := "lib-trabica-model",
  )
  .settings(
    libraryDependencies ++=
      Lib.config ++
        Lib.catsEffect ++
        Lib.fs2 ++
        Lib.decline ++
        Lib.scribe
  )
  .dependsOn(`lib-trabica-proto`)

lazy val `lib-trabica-net` = project
  .in(file("mod/lib-trabica-net"))
  .settings(commonSettings)
  .settings(scalacOptions ++= fullScalaOptions)
  .settings(
    name := "lib-trabica-net",
  )
  .settings(
    libraryDependencies ++=
      Lib.config ++
        Lib.catsEffect ++
        Lib.fs2 ++
        Lib.scribe
  )
  .dependsOn(`lib-trabica-model`)

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
  .dependsOn(`lib-trabica-proto` % "protobuf")
  .dependsOn(`lib-trabica-model`)
  .dependsOn(`lib-trabica-net`)

lazy val `lib-trabica-store` = project
  .in(file("mod/lib-trabica-store"))
  .settings(commonSettings)
  .settings(scalacOptions ++= fullScalaOptions)
  .settings(
    name := "lib-trabica-store",
  )
  .settings(
    libraryDependencies ++=
      Lib.config ++
        Lib.catsEffect ++
        Lib.fs2 ++
        Lib.decline ++
        Lib.scribe
  )
  .dependsOn(`lib-trabica-model`)

lazy val `lib-trabica-node` = project
  .in(file("mod/lib-trabica-node"))
  .settings(commonSettings)
  .settings(scalacOptions ++= fullScalaOptions)
  .settings(
    name := "lib-trabica-node",
  )
  .settings(
    libraryDependencies ++=
      Lib.config ++
        Lib.catsEffect ++
        Lib.fs2 ++
        Lib.decline ++
        Lib.scribe
  )
  .dependsOn(`lib-trabica-model`)
  .dependsOn(`lib-trabica-net`)
  .dependsOn(`lib-trabica-store`)

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
      "--no-fallback",
      "-H:IncludeResources=(reference|application).conf$",
      "-H:+ReportExceptionStackTraces",
      // build-time
      "--initialize-at-build-time=org.slf4j.simple.SimpleLogger",
      "--initialize-at-build-time=org.slf4j.LoggerFactory",
      "--initialize-at-build-time=io.grpc.netty.shaded.io.netty.util.internal.logging.Slf4JLoggerFactory",
      // run-time
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.channel.DefaultFileRegion",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.channel.MultithreadEventLoopGroup",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.channel.unix.Errors",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.channel.unix.IovArray",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.channel.unix.Limits",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.channel.epoll.Epoll",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.channel.epoll.Native",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.channel.epoll.EpollEventLoop",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.channel.epoll.EpollEventArray",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.channel.kqueue.KQueue",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.channel.kqueue.KQueueEventLoop",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.channel.kqueue.KQueueEventArray",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.channel.kqueue.Native",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.handler.ssl.OpenSsl",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.handler.ssl.OpenSslPrivateKeyMethod",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.handler.ssl.ReferenceCountedOpenSslEngine",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.handler.ssl.OpenSslAsyncPrivateKeyMethod",
      // "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.handler.ssl.BouncyCastleAlpnSslUtils",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.internal.tcnative.AsyncSSLPrivateKeyMethod",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.internal.tcnative.CertificateVerifier",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.internal.tcnative.SSL",
      "--initialize-at-run-time=io.grpc.netty.shaded.io.netty.internal.tcnative.SSLPrivateKeyMethod",
      "--initialize-at-run-time=io.netty.util.internal.logging.InternalLoggerFactory",
    )
  )
  .settings(
    libraryDependencies ++= Lib.decline,
  )
  .dependsOn(`lib-trabica-node`)
  .dependsOn(`lib-trabica-rpc`)
  .settings(List(Compile / mainClass := Some("trabica.node.Service")))

addCommandAlias("native", "GraalVMNativeImage/packageBin")
