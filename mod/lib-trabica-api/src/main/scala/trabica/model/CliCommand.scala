package trabica.model

import cats.data.*
import cats.syntax.all.*
import com.monovore.decline.*

sealed trait CliCommand {
  def host: String
  def port: Int
}

object CliCommand {

  case class Bootstrap(host: String, port: Int) extends CliCommand

  private final def bootstrapOpts: Opts[Bootstrap] =
    Opts.subcommand("bootstrap", "start a new trabica cluster with this initial bootstrap node") {
      val hostOpt = Opts.option[String]("host", "node host")
      val portOpt = Opts.option[Int]("port", "node port")
      (hostOpt, portOpt).mapN {
        case (host, port) => Bootstrap(host, port)
      }
    }

  case class Join(host: String, port: Int, peerHost: String, peerPort: Int) extends CliCommand

  private final def joinOpts: Opts[Join] =
    Opts.subcommand("join", "join a trabica cluster") {
      val hostOpt = Opts.option[String]("host", "node host")
      val portOpt = Opts.option[Int]("port", "node port")

      val peerAddressOpt: Opts[(String, Int)] =
        Opts
          .option[String]("peer-address", "peer address to join of the form host:port")
          .mapValidated { address =>
            address.trim.split(":") match {
              case Array(host, port) => port.toIntOption match {
                  case Some(value) =>
                    Validated.validNel((host, value))
                  case None =>
                    Validated.invalidNel(s"invalid port")
                }
              case _ =>
                Validated.invalidNel(s"invalid address `$address`")
            }
          }

      (hostOpt, portOpt, peerAddressOpt).mapN {
        case (host, port, (peerHost, peerPort)) => Join(host, port, peerHost, peerPort)
      }
    }

  def parse: Opts[CliCommand] =
    bootstrapOpts.orElse(joinOpts)
}
