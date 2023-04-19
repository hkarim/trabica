package trabica.model

import io.circe.*
import io.circe.syntax.*

sealed trait Request {
  def header: Header
  def widen: Request = this
}

object Request {

  given Encoder[Request] =
    Encoder.instance {
      case v: AppendEntries =>
        Encoder[AppendEntries].apply(v)
      case v: RequestVote =>
        Encoder[RequestVote].apply(v)
    }

  given Decoder[Request] =
    Decoder[Tag].at("header").at("tag").flatMap {
      case Tag.Request.AppendEntries =>
        Decoder[AppendEntries].map(_.widen)
      case Tag.Request.RequestVote =>
        Decoder[RequestVote].map(_.widen)
      case otherwise =>
        Decoder.failed(DecodingFailure(s"unrecognized request tag `$otherwise`", Nil))
    }

  final case class AppendEntries(
    id: MessageId,
    term: Term,
    leaderId: LeaderId,
    prevLogIndex: Index,
    prevLogTerm: Term,
    entries: Vector[Json],
    leaderCommitIndex: Index,
  ) extends Request {
    val header: Header = Header(
      id = id,
      version = Version.V1_0,
      tag = Tag.Request.AppendEntries,
      term = term,
    )
  }

  object AppendEntries {

    given Encoder[AppendEntries] =
      Encoder.instance { v =>
        Json.obj(
          "header"            -> v.header.asJson,
          "leaderId"          -> v.leaderId.asJson,
          "prevLogIndex"      -> v.prevLogIndex.asJson,
          "prevLogTerm"       -> v.prevLogTerm.asJson,
          "entries"           -> v.entries.asJson,
          "leaderCommitIndex" -> v.leaderCommitIndex.asJson,
        )
      }

    given Decoder[AppendEntries] =
      for {
        id                <- Decoder[MessageId].at("id")
        term              <- Decoder[Term].at("term")
        leaderId          <- Decoder[LeaderId].at("leaderId")
        prevLogIndex      <- Decoder[Index].at("prevLogIndex")
        prevLogTerm       <- Decoder[Term].at("prevLogTerm")
        entries           <- Decoder[Vector[Json]].at("entries")
        leaderCommitIndex <- Decoder[Index].at("leaderCommitIndex")
      } yield AppendEntries(
        id = id,
        term = term,
        leaderId = leaderId,
        prevLogIndex = prevLogIndex,
        prevLogTerm = prevLogTerm,
        entries = entries,
        leaderCommitIndex = leaderCommitIndex,
      )
  }

  final case class RequestVote(
    id: MessageId,
    term: Term,
    candidateId: CandidateId,
    lastLogIndex: Index,
    lastLogTerm: Term,
  ) extends Request {
    val header: Header = Header(
      id = id,
      version = Version.V1_0,
      tag = Tag.Request.RequestVote,
      term = term,
    )
  }

  object RequestVote {

    given Encoder[RequestVote] =
      Encoder.instance { v =>
        Json.obj(
          "header"       -> v.header.asJson,
          "candidateId"  -> v.candidateId.asJson,
          "lastLogIndex" -> v.lastLogIndex.asJson,
          "lastLogTerm"  -> v.lastLogTerm.asJson,
        )
      }

    given Decoder[RequestVote] =
      for {
        id           <- Decoder[MessageId].at("id")
        term         <- Decoder[Term].at("term")
        candidateId  <- Decoder[CandidateId].at("candidateId")
        lastLogIndex <- Decoder[Index].at("lastLogIndex")
        lastLogTerm  <- Decoder[Term].at("lastLogTerm")
      } yield RequestVote(
        id = id,
        term = term,
        candidateId = candidateId,
        lastLogIndex = lastLogIndex,
        lastLogTerm = lastLogTerm,
      )
  }
}
