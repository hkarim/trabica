package trabica.store

import cats.effect.*
import com.google.protobuf.{ByteString, CodedInputStream, CodedOutputStream}
import fs2.*
import trabica.model.{Index, LogEntry, LogEntryTag, NodeError}

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, Path, StandardOpenOption}
import java.util

trait FsmStore {
  def configuration: IO[Option[LogEntry]]
  def stream: Stream[IO, LogEntry]
  def at(index: Index): IO[LogEntry]
  def append(entry: LogEntry): IO[Unit]
  def truncate(keep: Index): IO[Unit]
}

object FsmStore {

  private class IndexFileStore(writeChannel: FileChannel, readChannel: FileChannel) {

    def logEntryAppended(at: Long): IO[Unit] =
      IO.blocking {
        val buffer = ByteBuffer.allocate(8)
        buffer.mark()
        buffer.putLong(at)
        buffer.reset()
        writeChannel.write(buffer)
      }.void

    def positionOf(index: Index): IO[Long] =
      if index == Index.zero then
        IO.raiseError(NodeError.ValueError(s"position of index 0 requested"))
      else
        IO
          .blocking {
            val p: Long = (index.value - 1) * 8
            val buffer  = ByteBuffer.allocate(8)
            buffer.mark()
            readChannel.read(buffer, p)
            buffer.reset().getLong
          }

    def truncate(keepIndex: Index): IO[Unit] =
      IO.blocking {
        val size: Long = if keepIndex == Index.zero then 0 else keepIndex.value * 8
        writeChannel.truncate(size)
      }.void

    def size: IO[Long] = IO.blocking(writeChannel.size())
  }

  private class FsmFileStore(
    writeChannel: FileChannel,
    readChannel: FileChannel,
    indexStore: IndexFileStore
  ) extends FsmStore {

    override def configuration: IO[Option[LogEntry]] = ???

    override def stream: Stream[IO, LogEntry] =
      Stream
        .eval(indexStore.size)
        .map(_ / 8)
        .flatMap { n =>
          Stream
            .range(1L, n + 1L)
            .evalMap(i => at(Index.from(i)))
        }

    override def at(index: Index): IO[LogEntry] =
      indexStore.positionOf(index).flatMap { position =>
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
      }

    override def append(entry: LogEntry): IO[Unit] = IO.blocking {
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
      writeChannel.position - size
    }.flatMap(indexStore.logEntryAppended)

    override def truncate(keepIndex: Index): IO[Unit] =
      indexStore.positionOf(keepIndex.increment).flatMap { position =>
        for {
          _ <- IO.blocking(writeChannel.truncate(position))
          _ <- indexStore.truncate(keepIndex)
        } yield ()
      }
  }

  def resource(dataDirectory: String): Resource[IO, FsmStore] = {
    val rootPath      = Path.of(dataDirectory)
    val indexPath     = rootPath.resolve("index.trabica").normalize
    val logPath       = rootPath.resolve("log.trabica").normalize
    val appendOptions = util.EnumSet.of(StandardOpenOption.CREATE, StandardOpenOption.APPEND)
    val readOptions   = util.EnumSet.of(StandardOpenOption.READ)
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
      indexWrite <- indexWriteChannel
      indexRead  <- indexReadChannel
      indexStore = new IndexFileStore(indexWrite, indexRead)
      logWrite <- logWriteChannel
      logRead  <- logReadChannel
      logStore = new FsmFileStore(logWrite, logRead, indexStore)
    } yield logStore
  }

}

object Delme extends IOApp.Simple {
  override val run: IO[Unit] =
    FsmStore.resource("var/00").use { store =>
      import cats.syntax.all.*
      (1 to 1000)
        .toList
        .traverse { i =>
          val s =
            if i % 2 == 0 then
              ByteString.copyFromUtf8("the long and winding road")
            else
              ByteString.copyFromUtf8("that leads to your door")
          store.append(LogEntry(index = i, term = 1, tag = LogEntryTag.Data, data = s))
        }
        .flatMap { _ =>
          store.truncate(Index.from(100L))
        }
        .flatMap { _ =>
          val print = store.stream.evalTap(IO.println).compile.drain
            for {
              f1 <- print.start
              f2 <- print.start
              _ <- f1.join
              _ <- f2.join
            } yield ()
        }
    }
}
