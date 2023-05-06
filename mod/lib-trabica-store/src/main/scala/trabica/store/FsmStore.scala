package trabica.store

import cats.effect.*
import fs2.*
import trabica.model.*

trait FsmStore {
  def bootstrap: IO[Unit]
  def configuration: IO[Option[LogEntry]]
  def readState: IO[Option[LocalState]]
  def writeState(state: LocalState): IO[Unit]
  def last: IO[Option[LogEntry]]
  def stream: Stream[IO, LogEntry]
  def streamFrom(index: Index): Stream[IO, LogEntry]
  def atIndex(index: Index): IO[LogEntry]
  def contains(index: Index, term: Term): IO[Boolean]
  def append(entry: LogEntry): IO[AppendResult]
  def truncate(keepIndex: Index): IO[Unit]
}
