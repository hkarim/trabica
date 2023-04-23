package trabica.model

import scodec.Codec

final case class Header(
  peer: Peer,
  messageId: MessageId,
  term: Term,
) derives Codec
