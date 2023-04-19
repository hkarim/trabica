package trabica.model

import io.circe.*

opaque type Tag = String
object Tag {
  given Encoder[Tag] = Encoder.encodeString
  given Decoder[Tag] = Decoder.decodeString

  object Request {
    final val AppendEntries: Tag = "Rq.AppendEntries"
    final val RequestVote: Tag   = "Rq.RequestVote"
  }

  object Response {
    final val AppendEntries: Tag = "Rs.AppendEntries"
    final val RequestVote: Tag   = "Rs.RequestVote"
  }

}


 
