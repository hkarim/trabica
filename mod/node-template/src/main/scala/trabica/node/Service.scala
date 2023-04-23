package trabica.node

import cats.effect.*
import com.monovore.decline.Opts
import com.monovore.decline.effect.CommandIOApp
import trabica.context.DefaultNodeContext
import trabica.model.CliCommand

object Service extends CommandIOApp(
    name = "trabica-node",
    header = "trabica node controller",
    version = "0.0.1"
  ) {

  override def main: Opts[IO[ExitCode]] =
    CliCommand.parse.map { command =>
      DefaultNodeContext
        .run(command)
        .as(ExitCode.Success)
    }

}
