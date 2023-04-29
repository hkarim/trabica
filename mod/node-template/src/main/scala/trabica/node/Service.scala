package trabica.node

import cats.effect.*
import com.monovore.decline.Opts
import com.monovore.decline.effect.CommandIOApp
import trabica.model.CliCommand

object Service
  extends CommandIOApp(name = "trabica-node", header = "trabica node", version = "0.0.1") {

  override def main: Opts[IO[ExitCode]] =
    CliCommand.parse.map { command =>
      Trabica
        .run(command)
        .as(ExitCode.Success)
    }

}
