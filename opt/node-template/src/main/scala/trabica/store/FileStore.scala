package trabica.store

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.google.protobuf.{CodedInputStream, CodedOutputStream}
import fs2.Stream
import trabica.model.*

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, Path, StandardOpenOption}
import java.util

// very simple and quick FmsStore implementation, similar to an SS-Table
object FileStore {

  private class StateStore(channel: FileChannel) {

    def bootstrap: IO[Unit] =
      IO.blocking(channel.truncate(0L)).void

    def write(state: LocalState): IO[Unit] =
      IO.blocking {
        // remove existing state if any
        channel.truncate(0L)
        // compute the serialized state length
        val length = state.serializedSize
        val size   = 4 + length
        // allocate a buffer to hold [length,data]
        val buffer = ByteBuffer.allocate(size)
        buffer.mark()
        // write the length into the buffer
        buffer.putInt(length)
        // write the state into the buffer
        state.writeTo(CodedOutputStream.newInstance(buffer))
        // write the buffer into the channel
        buffer.reset()
        channel.write(buffer)
      }.void

    def read: IO[Option[LocalState]] =
      IO.blocking {
        if channel.size() == 0 then
          None
        else {
          // allocate a buffer to read the entry length
          val lengthBuffer = ByteBuffer.allocate(4) // 32-bits
          lengthBuffer.mark()
          // read the length from the channel at the index position
          channel.read(lengthBuffer, 0L)
          lengthBuffer.reset()
          val length = lengthBuffer.getInt
          // allocate a buffer of capacity `length`
          val stateBuffer = ByteBuffer.allocate(length)
          stateBuffer.mark()
          // read the entry from the channel
          channel.read(stateBuffer, 4)
          stateBuffer.reset()
          Some(LocalState.parseFrom(CodedInputStream.newInstance(stateBuffer)))
        }
      }
  }

  private class ConfStore(channel: FileChannel) {

    def bootstrap: IO[Unit] =
      IO.blocking(channel.truncate(0L)).void

    def write(entry: LogEntry): IO[Unit] =
      IO.blocking {
        // remove existing state if any
        channel.truncate(0L)
        // compute the serialized state length
        val length = entry.serializedSize
        val size   = 4 + length
        // allocate a buffer to hold [length,data]
        val buffer = ByteBuffer.allocate(size)
        buffer.mark()
        // write the length into the buffer
        buffer.putInt(length)
        // write the state into the buffer
        entry.writeTo(CodedOutputStream.newInstance(buffer))
        // write the buffer into the channel
        buffer.reset()
        channel.write(buffer)
      }.void

    def read: IO[Option[LogEntry]] =
      IO.blocking {
        if channel.size() == 0 then
          None
        else {
          // allocate a buffer to read the entry length
          val lengthBuffer = ByteBuffer.allocate(4) // 32-bits
          lengthBuffer.mark()
          // read the length from the channel at the index position
          channel.read(lengthBuffer, 0L)
          lengthBuffer.reset()
          val length = lengthBuffer.getInt
          // allocate a buffer of capacity `length`
          val stateBuffer = ByteBuffer.allocate(length)
          stateBuffer.mark()
          // read the entry from the channel
          channel.read(stateBuffer, 4)
          stateBuffer.reset()
          Some(LogEntry.parseFrom(CodedInputStream.newInstance(stateBuffer)))
        }
      }

    def truncate(upToIncluding: Index): IO[Unit] =
      read.flatMap {
        case Some(v) if v.index <= upToIncluding.value =>
          IO.blocking(channel.truncate(0L)).void
        case _ =>
          IO.unit
      }
  }

  private class IndexStore(writeChannel: FileChannel, readChannel: FileChannel) {

    def bootstrap: IO[Unit] =
      IO.blocking(writeChannel.truncate(0L)).void

    def size: IO[Long] =
      IO.blocking(readChannel.size())

    def logEntryAppended(position: Long): IO[Unit] =
      IO.blocking {
        val buffer = ByteBuffer.allocate(8)
        buffer.mark()
        buffer.putLong(position)
        buffer.reset()
        writeChannel.write(buffer)
      }.void

    def positionOf(index: Index): IO[Long] = {
      val p = (index.value - 1) * 8
      if p >= 0 then
        read(p)
      else
        IO.raiseError(NodeError.StoreError(s"invalid index position $p"))
    }

    def read(position: Long): IO[Long] =
      IO
        .blocking {
          val buffer = ByteBuffer.allocate(8)
          buffer.mark()
          val consumed = readChannel.read(buffer, position)
          val result   = buffer.reset().getLong
          (consumed, result)
        }
        .flatMap { (consumed, result) =>
          if consumed == 0 || consumed == -1 then
            IO.raiseError(NodeError.StoreError(s"invalid entry position $position"))
          else
            IO.pure(result)
        }

    def truncate(upToIncluding: Index): IO[Unit] =
      IO.blocking {
        val position   = (upToIncluding.value - 1) * 8
        val size: Long = if position - 8 < 0 then 0 else position - 8
        writeChannel.truncate(size)
      }.void

    def streamFrom(position: Long): Stream[IO, Long] = {
      val first = if position < 0 then 0 else position
      Stream
        .eval(size)
        .flatMap { n =>
          Stream
            .range(first, n, 8L)
            .evalMap(read)
        }
    }

  }

  private class LogStore(
    writeChannel: FileChannel,
    readChannel: FileChannel,
    stateStore: StateStore,
    indexStore: IndexStore,
    confStore: ConfStore,
  ) extends Log {

    override def bootstrap: IO[Unit] =
      for {
        _ <- confStore.bootstrap
        _ <- stateStore.bootstrap
        _ <- indexStore.bootstrap
        _ <- IO.blocking(writeChannel.truncate(0L)).void
      } yield ()

    override def configuration: IO[Option[LogEntry]] =
      confStore.read

    override def readState: IO[Option[LocalState]] =
      stateStore.read

    override def writeState(state: LocalState): IO[Unit] =
      stateStore.write(state)

    override def last: IO[Option[LogEntry]] =
      for {
        indexSize <- indexStore.size
        indexPosition = indexSize - 8
        entry <-
          if indexPosition < 0 then
            IO.pure(None)
          else
            for {
              logPosition <- indexStore.read(indexPosition)
              e           <- read(logPosition)
            } yield Some(e)
      } yield entry

    override def stream: Stream[IO, LogEntry] =
      streamFrom(Index.one)

    override def streamFrom(index: Index): Stream[IO, LogEntry] =
      indexStore
        .streamFrom((index.value - 1) * 8)
        .evalMap(read)

    override def atIndex(index: Index): IO[LogEntry] =
      indexStore
        .positionOf(index)
        .flatMap(read)

    override def contains(index: Index, term: Term): IO[Boolean] = {
      val io = for {
        entry <- atIndex(index)
        valid = entry.term == term.value
      } yield valid
      io.handleErrorWith(_ => IO.pure(false))
    }

    private def read(position: Long): IO[LogEntry] =
      IO
        .blocking {
          // allocate a buffer to read the entry length
          val lengthBuffer = ByteBuffer.allocate(4) // 32-bits
          lengthBuffer.mark()
          // read the length from the channel at the index position
          readChannel.read(lengthBuffer, position)
          lengthBuffer.reset()
          val length = lengthBuffer.getInt
          // allocate a buffer of capacity `length`
          val entryBuffer = ByteBuffer.allocate(length)
          entryBuffer.mark()
          // read the entry from the channel
          val consumed = readChannel.read(entryBuffer, position + 4)
          entryBuffer.reset()
          val entry = LogEntry.parseFrom(CodedInputStream.newInstance(entryBuffer))
          (consumed, entry)
        }
        .flatMap { (consumed, entry) =>
          if consumed == 0 || consumed == -1 then
            IO.raiseError(NodeError.StoreError(s"invalid entry position $position"))
          else
            IO.pure(entry)
        }

    private def write(entry: LogEntry): IO[Unit] =
      IO
        .blocking {
          // compute the serialized entry length
          val length = entry.serializedSize
          val size   = 4 + length
          // allocate a buffer to hold [length,data]
          val buffer = ByteBuffer.allocate(size)
          buffer.mark()
          // write the length into the buffer
          buffer.putInt(length)
          // write the entry into the buffer
          entry.writeTo(CodedOutputStream.newInstance(buffer))
          // write the buffer into the channel
          buffer.reset()
          writeChannel.write(buffer)
          // get the position of this entry
          writeChannel.position - size
        }
        .flatMap { p =>
          // index the position of this entry
          indexStore.logEntryAppended(p)
        }
        .flatMap { _ =>
          // if this entry is a configuration
          // save it as the latest
          if entry.tag == LogEntryTag.Conf then
            confStore.write(entry)
          else
            IO.unit
        }

    override def append(entry: LogEntry): IO[AppendResult] =
      for {
        lastEntry <- last
        existingEntry <-
          atIndex(Index.of(entry.index))
            .map(_.some)
            .recover(_ => Option.empty[LogEntry])
        r <- (lastEntry, existingEntry) match {
          case (None, None) =>
            // no entries in the log, first index must be 1
            if entry.index == 1L then
              write(entry).map(_ => AppendResult.Appended)
            else
              IO.pure(
                AppendResult.NonMonotonicIndex(
                  storeIndex = Index.zero,
                  incomingIndex = Index.of(entry.index),
                )
              )
          case (Some(e), None) =>
            // last entry exists, but no existing entry with the incoming index
            // check that the index is strictly monotonic
            // and the term is the same or of a higher value than the last one
            if e.index + 1 != entry.index then
              IO.pure(
                AppendResult.NonMonotonicIndex(
                  storeIndex = Index.of(e.index),
                  incomingIndex = Index.of(entry.index),
                )
              )
            else if e.term > entry.term then
              IO.pure(
                AppendResult.HigherTermExists(
                  storeTerm = Term.of(e.term),
                  incomingTerm = Term.of(entry.term),
                )
              )
            else
              write(entry).map(_ => AppendResult.Appended)
          case (_, Some(e)) =>
            // entry with the same index exists
            // check the term for conflicts
            if e.term != entry.term then {
              // terms are not the same, conflict
              IO.pure(
                AppendResult.IndexExistsWithTermConflict(
                  storeTerm = Term.of(e.term),
                  incomingTerm = Term.of(entry.term)
                )
              )
            } else {
              // terms are the same
              IO.pure(AppendResult.IndexExists)
            }
        }
      } yield r

    override def truncate(upToIncluding: Index): IO[Unit] =
      if upToIncluding == Index.zero || upToIncluding == Index.one then {
        for {
          _ <- IO.blocking(writeChannel.truncate(0L))
          _ <- indexStore.truncate(upToIncluding)
          _ <- confStore.truncate(upToIncluding)
        } yield ()
      } else {
        indexStore.positionOf(Index.of(upToIncluding.value - 1)).flatMap { position =>
          for {
            _ <- IO.blocking(writeChannel.truncate(position))
            _ <- indexStore.truncate(upToIncluding)
            _ <- confStore.truncate(upToIncluding)
          } yield ()
        }
      }

  }

  def resource(dataDirectory: String): Resource[IO, Log] = {
    val rootPath  = Path.of(dataDirectory)
    val indexPath = rootPath.resolve("index.trabica").normalize
    val logPath   = rootPath.resolve("log.trabica").normalize
    val statePath = rootPath.resolve("state.trabica").normalize
    val confPath  = rootPath.resolve("conf.trabica").normalize

    val readWriteOptions = util.EnumSet.of(
      StandardOpenOption.CREATE,
      StandardOpenOption.READ,
      StandardOpenOption.WRITE
    )
    val appendOptions = util.EnumSet.of(StandardOpenOption.CREATE, StandardOpenOption.APPEND)
    val readOptions   = util.EnumSet.of(StandardOpenOption.READ)

    val stateChannel = Resource.fromAutoCloseable {
      IO.blocking(Files.createDirectories(rootPath)) >>
        IO.blocking(FileChannel.open(statePath, readWriteOptions))
    }
    val confChannel = Resource.fromAutoCloseable {
      IO.blocking(Files.createDirectories(rootPath)) >>
        IO.blocking(FileChannel.open(confPath, readWriteOptions))
    }
    val indexWriteChannel = Resource.fromAutoCloseable {
      IO.blocking(Files.createDirectories(rootPath)) >>
        IO.blocking(FileChannel.open(indexPath, appendOptions))
    }
    val indexReadChannel = Resource.fromAutoCloseable {
      IO.blocking(Files.createDirectories(rootPath)) >>
        IO.blocking(FileChannel.open(indexPath, readOptions))
    }
    val logWriteChannel = Resource.fromAutoCloseable {
      IO.blocking(Files.createDirectories(rootPath)) >>
        IO.blocking(FileChannel.open(logPath, appendOptions))
    }
    val logReadChannel = Resource.fromAutoCloseable {
      IO.blocking(Files.createDirectories(rootPath)) >>
        IO.blocking(FileChannel.open(logPath, readOptions))
    }

    for {
      confReadWrite <- confChannel
      confStore = new ConfStore(confReadWrite)
      stateReadWrite <- stateChannel
      stateStore = new StateStore(stateReadWrite)
      indexWrite <- indexWriteChannel
      indexRead  <- indexReadChannel
      indexStore = new IndexStore(indexWrite, indexRead)
      logWrite <- logWriteChannel
      logRead  <- logReadChannel
      logStore = new LogStore(logWrite, logRead, stateStore, indexStore, confStore)
    } yield logStore
  }
}
