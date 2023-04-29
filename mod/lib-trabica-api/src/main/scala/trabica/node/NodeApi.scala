package trabica.node

import cats.effect.*
import trabica.rpc.*

trait NodeApi {

  def appendEntries(request: AppendEntriesRequest): IO[Boolean]

  def vote(request: VoteRequest): IO[Boolean]

  def join(request: JoinRequest): IO[JoinResponse.Status]

}
