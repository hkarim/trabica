package trabica.model

import cats.data.*
import cats.syntax.all.*
import com.comcast.ip4s.*
import com.monovore.decline.*

sealed trait CliCommand

object CliCommand {

  private def readPort(raw: Int): ValidatedNel[String, Port] =
    Port.fromInt(raw) match {
      case Some(value) =>
        Validated.validNel(value)
      case None =>
        Validated.invalidNel(s"invalid port `$raw`")
    }

  private def readIp(raw: String): ValidatedNel[String, Ipv4Address] =
    Ipv4Address.fromString(raw) match {
      case Some(value) =>
        Validated.validNel(value)
      case None =>
        Validated.invalidNel(s"invalid ip address `$raw`")
    }

  case class Bootstrap(ip: Ipv4Address, port: Port) extends CliCommand

  private final def bootstrapOpts: Opts[Bootstrap] =
    Opts.subcommand("bootstrap", "start a new trabica cluster with this initial bootstrap node") {
      val ipOpt =
        Opts
          .option[String]("ip", "node ip address")
          .mapValidated(readIp)
      val portOpt =
        Opts
          .option[Int]("port", "node port")
          .mapValidated(readPort)
      (ipOpt, portOpt).mapN {
        case (ip, port) => Bootstrap(ip, port)
      }
    }

  case class Join(ip: Ipv4Address, port: Port, peerIp: Ipv4Address, peerPort: Port) extends CliCommand

  private final def joinOpts: Opts[Join] =
    Opts.subcommand("join", "join a trabica cluster") {
      val ipOpt =
        Opts
          .option[String]("ip", "node ip address")
          .mapValidated(readIp)
      val portOpt =
        Opts
          .option[Int]("port", "node port")
          .mapValidated(readPort)

      val peerAddressOpt: Opts[(Ipv4Address, Port)] =
        Opts
          .option[String]("peer-address", "peer address to join of the form ip:port")
          .mapValidated { address =>
            address.trim.split(":") match {
              case Array(ip, port) => port.toIntOption match {
                  case Some(value) =>
                    (readIp(ip), readPort(value)).mapN {
                      case (l, r) => (l, r)
                    }
                  case None =>
                    Validated.invalidNel(s"invalid port")
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
