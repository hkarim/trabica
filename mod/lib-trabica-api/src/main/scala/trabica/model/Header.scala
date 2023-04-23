package trabica.model

import scodec.Codec

final case class Header(
  id: MessageId,
  term: Term,
) derives Codec
