package trabica.model

import scodec.*

case class Version(
  major: Byte,
  minor: Byte,
)

object Version {
  given Codec[Version] = (codecs.byte :: codecs.byte).as[Version]
  final val V1_0: Version = Version(1, 0)
}
