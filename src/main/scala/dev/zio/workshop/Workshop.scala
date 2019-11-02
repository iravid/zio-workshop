package dev.zio.workshop

import java.util.concurrent.TimeUnit

import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.S3ObjectSummary
import zio._
import zio.blocking._
import zio.stream._
import zio.console._
import zio.duration._
import zio.clock._

import scala.collection.JavaConverters._
import com.amazonaws.services.s3.model.ListObjectsV2Request
import java.io.IOException

object Workshop2 extends App {
  def run(args: List[String]): ZIO[ZEnv, Nothing, Int] = UIO.succeed(0)

  // Example code will be at https://github.com/iravid/zio-workshop

  ///////////////////////////////////////////////////////
  // ZManaged

  trait KafkaConsumer {
    def close: UIO[Unit]
  }
  def makeKafka: Task[KafkaConsumer] = ???

  def makeKafkaManaged: Managed[Throwable, KafkaConsumer] =
    makeKafka.toManaged(_.close)

  val program =
    makeKafka.bracket(consumer => consumer.close) { consumer =>
      putStrLn(consumer.toString)
    }

  val programManaged =
    makeKafkaManaged.use { consumer =>
      putStrLn(consumer.toString)
    }

  trait HTTPServer {
    def close: UIO[Unit]
  }
  def makeHTTP: UIO[HTTPServer] = ???
  def makeHttpManaged(consumer: KafkaConsumer) = {
    val _ = consumer
    makeHTTP.toManaged(_.close)
  }

  val program2 =
    makeKafka.bracket(consumer => consumer.close) { consumer =>
      makeHTTP.bracket(_.close) { http =>
        putStrLn(http.toString + consumer.toString)
      }
    }

  // val program2Managed =
  //   makeKafkaManaged.use { consumer =>
  //     makeHttpManaged.use { http =>
  //       putStrLn(http.toString + consumer.toString)
  //     }
  //   }

  case class AppResources(http: HTTPServer, consumer: KafkaConsumer)
  val appResources: Managed[Throwable, AppResources] =
    for {
      consumers              <- (makeKafkaManaged zipPar makeKafkaManaged)
      (consumer1, consumer2) = consumers
      http                   <- makeHttpManaged(consumer1)
    } yield AppResources(http, consumer1)

  val app =
    appResources.useForever

  trait ChangeableResource
  val changeable = ZManaged.switchable[Blocking, Throwable, ChangeableResource]

  ////////////////////////////////////////////////////////
  // ZStream
  val literalStream: Stream[Nothing, Int] = Stream(1, 2, 3)

  val vectorStream: Stream[Nothing, Int] = Stream.fromIterable(Vector(4, 5, 6))

  val readLine: ZStream[Console, IOException, String] =
    ZStream.fromEffect(getStrLn)

  val readLines =
    ZStream
      .repeatEffect(getStrLn)
      .take(10)

  val repeatingEffect: ZStream[Clock, Nothing, Long] =
    ZStream.repeatEffect(currentTime(TimeUnit.MILLISECONDS))

  val repeatingEffectWithDelay =
    ZStream.repeatEffectWith(ZIO.unit, ZSchedule.spaced(30.seconds))

  val concatenatedStream =
    readLines ++
      ZStream.fromEffect(putStrLn("now another") *> getStrLn) ++
      ZStream.fromEffect(putStrLn("done reading")).drain ++
      ZStream("a", "b", "c")

  def read10Lines =
    ZIO.collectAll(List.fill(10)(getStrLn))

  val composed: ZStream[Console, Throwable, String] =
    for {
      line      <- readLines
      num       <- ZStream.fromEffect(Task(line.toInt))
      moreLines <- ZStream.repeatEffect(getStrLn).take(num)
    } yield moreLines

  val s3 = Task(AmazonS3ClientBuilder.defaultClient())

  val fetchFirst: RIO[Blocking, (List[S3ObjectSummary], Option[String])] =
    s3.flatMap { s3 =>
      effectBlocking(s3.listObjectsV2("bucket")).map { result =>
        (result.getObjectSummaries.asScala.toList, Option(result.getNextContinuationToken()))
      }
    }

  def fetchNext(token: String): RIO[Blocking, (List[S3ObjectSummary], Option[String])] =
    s3.flatMap { s3 =>
      effectBlocking(
        s3.listObjectsV2(
          new ListObjectsV2Request()
            .withBucketName("bucket")
            .withContinuationToken(token)
        )
      ).map { result =>
        (result.getObjectSummaries.asScala.toList, Option(result.getNextContinuationToken()))
      }
    }

  val bucketListing = ZStream.fromEffect(fetchFirst).flatMap {
    case (summaries, None) => ZStream.fromIterable(summaries)
    case (summaries, Some(token)) =>
      ZStream.fromIterable(summaries) ++
        ZStream.paginate(token)(fetchNext).mapConcat(identity)
  }

  trait RabbitMQ {
    def register(onMessage: Int => Unit, onDone: () => Unit, onError: Throwable => Unit): Unit
  }

  class RabbitMQImpl extends RabbitMQ {
    def register(onMessage: Int => Unit, onDone: () => Unit, onError: Throwable => Unit): Unit = {
      import scala.concurrent.ExecutionContext.Implicits.global
      import scala.concurrent.Future
      Future {
        (1 to 10).foreach { i =>
          onMessage(i)
          Thread.sleep(2000)
        }

        onDone()
      }

      ()
    }
  }

  val rabbitMQ: RabbitMQ = new RabbitMQImpl

  def md5Sum(i: Int): UIO[Int] = ???

  val streamOfMessages: Stream[Throwable, Int] =
    Stream
      .effectAsync[Throwable, Int] { cb =>
        rabbitMQ.register(
          onMessage = msg => cb(UIO.succeed(msg)),
          onDone = () => cb(ZIO.fail(None)),
          onError = e => cb(ZIO.fail(Some(e)))
        )
      }

  val msgAndTimestamp = streamOfMessages.zip(repeatingEffect)

  def readObject(bucket: String, key: String): ZStream[Blocking, Throwable, Chunk[Byte]] =
    ZStream.unwrap {
      val r = for {
        s3Client <- s3
        obj      <- effectBlocking(s3Client.getObject(bucket, key))
        stream = ZStream
          .bracket(effectBlocking(obj.getObjectContent()))(content => UIO(content.close()))
          .flatMap(ZStream.fromInputStream(_).chunks)
      } yield stream

      r
    }

  def appendToFile(bytes: Chunk[Byte], filename: String): RIO[Blocking, Unit] =
    UIO(println(s"Writing ${bytes.size} bytes to $filename"))

  def canFail(summ: S3ObjectSummary): Task[S3ObjectSummary] = ???

  val parallelFilesFromS3: ZIO[Blocking, Throwable, Int] =
    ZStream
      .fromEffect(fetchFirst)
      .flatMap {
        case (summaries, None) => ZStream.fromIterable(summaries)
        case (summaries, Some(token)) =>
          ZStream.fromIterable(summaries) ++
            ZStream.paginate(token)(fetchNext).mapConcat(identity)
      }
      .buffer(10)
      .flatMapPar(10) { objectSummary =>
        val filename = objectSummary.getKey

        readObject(objectSummary.getBucketName, objectSummary.getKey)
          .mapM(chunk => appendToFile(chunk, filename).as(chunk))
          .map(_.size)
      }
      .run(ZSink.foldLeft(0)((bytesWritten, bytes: Int) => bytesWritten + bytes))
}

object OtherExamples {
  def run(args: List[String]): ZIO[ZEnv, Nothing, Int] = {
    val _        = args
    val rabbitMq = new RabbitMQ

    def firstSchedule: ZSchedule[Clock, Option[List[Int]], Unit] =
      ZSchedule
        .spaced(5.seconds)
        .whileInput { data: Option[List[Int]] =>
          data match {
            case None => true
            case Some(data) =>
              data.size <= 100
          }
        }
        .unit
        .andThen(secondSchedule.unit)

    def secondSchedule: ZSchedule[Clock, Option[List[Int]], Unit] =
      ZSchedule
        .spaced(250.millis)
        .whileInput { data: Option[List[Int]] =>
          data match {
            case None => false
            case Some(data) =>
              data.size > 100
          }
        }
        .unit
        .andThen(firstSchedule)

    val stream: ZStream[Clock with Console, Nothing, List[Int]] =
      ZStream
        .effectAsync[Any, Nothing, Int] { cb =>
          rabbitMq.register(
            i => cb(ZIO.succeed(i)),
            () => cb(ZIO.fail(None))
          )
        }
        .aggregateWithin(
          ZSink
            .foldUntil(List[Int](), 150) { (acc, el: Int) =>
              el :: acc
            }
            .map(_.reverse),
          firstSchedule
        )
        .mapMPar(5) { el: List[Int] =>
          putStrLn(s"Evaluating $el").delay(3.seconds).as(el)
        }

    stream.runCollect.flatMap(l => putStrLn(l.toString)).as(0).orDie
  }

  class RabbitMQ {
    def register(listener: Int => Unit, done: () => Unit): Unit =
      unsafeRunAsync_(
        ZIO
          .effect(listener(10))
          .repeat(
            Schedule.recurs(2) && ZSchedule.spaced(1.seconds)
          ) *> ZIO.effect(done())
      )
  }
}
