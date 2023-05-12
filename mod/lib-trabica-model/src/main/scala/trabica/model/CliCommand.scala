package trabica.model

import cats.syntax.all.*
import com.monovore.decline.*

sealed trait CliCommand {
  def dataDirectory: String
}

object CliCommand {

  case class Bootstrap(
    id: String,
    host: String,
    port: Int,
    dataDirectory: String,
    members: Vector[Member],
  ) extends CliCommand

  private final def bootstrapOpts: Opts[Bootstrap] =
    Opts.subcommand("bootstrap", "start a new trabica node in bootstrap mode") {
      val idOpt    = Opts.option[String]("id", "node id")
      val hostOpt  = Opts.option[String]("host", "node host")
      val portOpt  = Opts.option[Int]("port", "node port")
      val dirOpt   = Opts.option[String]("data", "data directory")
      val nodesOpt = Opts.options[String]("peer", "member of the form id@host:port")
      (idOpt, hostOpt, portOpt, dirOpt, nodesOpt).mapN {
        case (id, host, port, dir, nodes) =>
          val quorumPeers = nodes.map { raw =>
            val split   = raw.split("@")
            val id      = split(0)
            val address = split(1).split(":")
            val host    = address(0)
            val port    = address(1).toInt
            Member(id = id, peer = Some(Peer(host = host, port = port)))
          }
          Bootstrap(id, host, port, dir, quorumPeers.toList.toVector)
      }
    }

  case class Startup(
    id: String,
    host: String,
    port: Int,
    dataDirectory: String
  ) extends CliCommand

  private final def startupOpts: Opts[Startup] =
    Opts.subcommand("startup", "start a trabica node in operation mode") {
      val idOpt   = Opts.option[String]("id", "node id")
      val hostOpt = Opts.option[String]("host", "node host")
      val portOpt = Opts.option[Int]("port", "node port")
      val dirOpt  = Opts.option[String]("data", "data directory")
      (idOpt, hostOpt, portOpt, dirOpt).mapN {
        case (id, host, port, dir) =>
          Startup(id, host, port, dir)
      }
    }

  def parse: Opts[CliCommand] =
    bootstrapOpts.orElse(startupOpts)
}
