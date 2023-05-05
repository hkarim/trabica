package trabica.net

import cats.effect.IO
import trabica.model.*

trait NodeApi {

  def quorumId: String
  
  def quorumPeer: Peer
  
  def quorumNode: QuorumNode =
    QuorumNode(id = quorumId, peer = Some(quorumPeer))

  def appendEntries(request: AppendEntriesRequest): IO[AppendEntriesResponse]

  def vote(request: VoteRequest): IO[VoteResponse]

}
