package trabica.store

import cats.effect.{IO, Resource}
import com.google.protobuf.{CodedInputStream, CodedOutputStream}
import fs2.Stream
import trabica.model.{Index, LocalState, LogEntry, LogEntryTag}

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, Path, StandardOpenOption}
import java.util

// very simple and quick FmsStore implementation, similar to an SS-Table
object FsmFileStore {

  private class StateFileStore(channel: FileChannel) {

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

  private class IndexFileStore(writeChannel: FileChannel, readChannel: FileChannel) {

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

    def positionOf(index: Index): IO[Long] =
      read((index.value - 1) * 8)

    def read(position: Long): IO[Long] =
      IO.blocking {
        val buffer = ByteBuffer.allocate(8)
        buffer.mark()
        readChannel.read(buffer, position)
        buffer.reset().getLong
      }

    def truncate(keepIndex: Index): IO[Unit] =
      IO.blocking {
        val size: Long = if keepIndex == Index.zero then 0 else keepIndex.value * 8
        writeChannel.truncate(size)
      }.void

    def stream: Stream[IO, Long] =
      streamFrom(0L)

    def streamFrom(position: Long): Stream[IO, Long] =
      Stream
        .eval(size)
        .flatMap { n =>
          Stream
            .range(position, n, 8L)
            .evalMap(read)
        }

  }

  private class FsmFileStore(
    writeChannel: FileChannel,
    readChannel: FileChannel,
    stateStore: StateFileStore,
    indexStore: IndexFileStore
  ) extends FsmStore {

    override def bootstrap: IO[Unit] =
      for {
        _ <- stateStore.bootstrap
        _ <- indexStore.bootstrap
        _ <- IO.blocking(writeChannel.truncate(0L)).void
      } yield ()

    // TODO: should work backwards, would be more efficient
    override def configuration: IO[Option[LogEntry]] =
      stream
        .filter(_.tag == LogEntryTag.Conf)
        .compile
        .last

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

    private def read(position: Long): IO[LogEntry] =
      IO.blocking {
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
        readChannel.read(entryBuffer, position + 4)
        entryBuffer.reset()
        LogEntry.parseFrom(CodedInputStream.newInstance(entryBuffer))
      }

    override def append(entry: LogEntry): IO[Unit] =
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
          // index the position of thi entry
          indexStore.logEntryAppended(p)
        }

    override def truncate(keepIndex: Index): IO[Unit] =
      indexStore.positionOf(keepIndex.increment).flatMap { position =>
        for {
          _ <- IO.blocking(writeChannel.truncate(position))
          _ <- indexStore.truncate(keepIndex)
        } yield ()
      }
  }

  def resource(dataDirectory: String): Resource[IO, FsmStore] = {
    val rootPath  = Path.of(dataDirectory)
    val indexPath = rootPath.resolve("index.trabica").normalize
    val logPath   = rootPath.resolve("log.trabica").normalize
    val statePath = rootPath.resolve("state.trabica").normalize

    val readWriteOptions = util.EnumSet.of(
      StandardOpenOption.CREATE,
      StandardOpenOption.READ,
      StandardOpenOption.WRITE)
    val appendOptions    = util.EnumSet.of(StandardOpenOption.CREATE, StandardOpenOption.APPEND)
    val readOptions      = util.EnumSet.of(StandardOpenOption.READ)

    val stateChannel = Resource.fromAutoCloseable {
      IO.blocking(Files.createDirectories(rootPath)) >>
        IO.blocking(FileChannel.open(statePath, readWriteOptions))
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
      stateReadWrite <- stateChannel
      stateStore = new StateFileStore(stateReadWrite)
      indexWrite <- indexWriteChannel
      indexRead  <- indexReadChannel
      indexStore = new IndexFileStore(indexWrite, indexRead)
      logWrite <- logWriteChannel
      logRead  <- logReadChannel
      logStore = new FsmFileStore(logWrite, logRead, stateStore, indexStore)
    } yield logStore
  }
}
