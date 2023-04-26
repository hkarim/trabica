package trabica.model

import scodec.*

opaque type Tag = Long

object Tag {

  given Codec[Tag] = codecs.long(64)

  object Request {
    final val AppendEntries: Tag = 1 << 0
    final val RequestVote: Tag   = 1 << 1
    final val Join: Tag          = 1 << 2
  }

  object Response {
    final val AppendEntries: Tag = 1 << 16 | 1 << 0
    final val RequestVote: Tag   = 1 << 16 | 1 << 1
    final val Join: Tag          = 1 << 16 | 1 << 2
    final val Error: Tag         = 1 << 16 | 1 << 3
  }

}
