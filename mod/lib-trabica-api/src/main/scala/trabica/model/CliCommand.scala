package trabica.model

import cats.data.*
import cats.syntax.all.*
import com.monovore.decline.*

sealed trait CliCommand

object CliCommand {
  case class Bootstrap(ip: String, port: Int) extends CliCommand

  private final def bootstrapOpts: Opts[Bootstrap] =
    Opts.subcommand("bootstrap", "start a new trabica cluster with this initial bootstrap node") {
      val ipOpt   = Opts.option[String]("ip", "node ip address")
      val portOpt = Opts.option[Int]("port", "node port")
      (ipOpt, portOpt).mapN {
        case (ip, port) => Bootstrap(ip, port)
      }
    }

  case class Join(ip: String, port: Int, peerIp: String, peerPort: Int) extends CliCommand

  private final def joinOpts: Opts[Join] =
    Opts.subcommand("join", "join a trabica cluster") {
      val ipOpt   = Opts.option[String]("ip", "node ip address")
      val portOpt = Opts.option[Int]("port", "node port")

      val peerAddressOpt: Opts[(String, Int)] =
        Opts
          .option[String]("peer-address", "peer address to join of the form ip:port")
          .mapValidated { address =>
            address.trim.split(":") match {
              case Array(ip, port) => port.toIntOption match {
                  case Some(value) => Validated.validNel((ip, value))
                  case None        => Validated.invalidNel(s"invalid port")
                }
              case _ =>
                Validated.invalidNel(s"invalid address `$address`")
            }
          }
      (ipOpt, portOpt, peerAddressOpt).mapN {
        case (ip, port, (peerIp, peerPort)) => Join(ip, port, peerIp, peerPort)
      }
    }

  def parse: Opts[CliCommand] =
    bootstrapOpts.orElse(joinOpts)
}
