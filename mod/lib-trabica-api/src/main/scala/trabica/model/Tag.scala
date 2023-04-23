package trabica.model

import scodec.*

opaque type Tag = Int

object Tag {

  given Codec[Tag] = codecs.uint16

  object Request {
    final val AppendEntries: Tag = 1 << 0
    final val RequestVote: Tag   = 1 << 1
  }

  object Response {
    final val AppendEntries: Tag = 1 << 16 | 1 << 0
    final val RequestVote: Tag   = 1 << 16 | 1 << 1
  }

}
