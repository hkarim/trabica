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
    dataDirectory: String
  ) extends CliCommand

  private final def bootstrapOpts: Opts[Bootstrap] =
    Opts.subcommand("bootstrap", "start a new trabica node") {
      val idOpt   = Opts.option[String]("id", "node id")
      val hostOpt = Opts.option[String]("host", "node host")
      val portOpt = Opts.option[Int]("port", "node port")
      val dirOpt  = Opts.option[String]("data", "data directory")
      (idOpt, hostOpt, portOpt, dirOpt).mapN {
        case (id, host, port, dir) => Bootstrap(id, host, port, dir)
      }
    }

  case class Startup(dataDirectory: String) extends CliCommand

  private final def startupOpts: Opts[Startup] =
    Opts.subcommand("startup", "re-start a pre-configured trabica node") {
      val dirOpt = Opts.option[String]("data", "data directory")
      dirOpt.map(dir => Startup(dir))
    }

  def parse: Opts[CliCommand] =
    bootstrapOpts.orElse(startupOpts)
}
