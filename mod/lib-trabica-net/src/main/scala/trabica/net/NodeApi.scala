package trabica.net

import cats.effect.IO
import trabica.model.*

trait NodeApi {

  def memberId: String

  def memberPeer: Peer

  def quorumNode: Member =
    Member(id = memberId, peer = Some(memberPeer))

  def appendEntries(request: AppendEntriesRequest): IO[AppendEntriesResponse]

  def vote(request: VoteRequest): IO[VoteResponse]

  def addServer(request: AddServerRequest): IO[AddServerResponse]

  def removeServer(request: RemoveServerRequest): IO[RemoveServerResponse]

  def stepDown(request: StepDownRequest): IO[StepDownResponse]

}
