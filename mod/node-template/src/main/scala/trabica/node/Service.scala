package trabica.node

import cats.effect.*
import trabica.context.DefaultNodeContext

object Service extends IOApp.Simple {
  override val run: IO[Unit] =
    DefaultNodeContext.run
}
