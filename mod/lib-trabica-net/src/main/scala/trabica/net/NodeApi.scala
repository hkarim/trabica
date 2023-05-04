package trabica.net

import cats.effect.IO
import trabica.model.*

trait NodeApi {

  def peer: Peer

  def appendEntries(request: AppendEntriesRequest): IO[AppendEntriesResponse]

  def vote(request: VoteRequest): IO[VoteResponse]

  def join(request: JoinRequest): IO[JoinResponse]

}
