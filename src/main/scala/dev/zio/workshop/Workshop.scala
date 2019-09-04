package dev.zio.workshop

import zio._
import zio.stream._
import zio.console._
import zio.duration._
import zio.clock.Clock

object Workshop extends App {
  def run(args: List[String]): ZIO[Environment, Nothing, Int] = {
    val rabbitMq = new RabbitMQ

    def firstSchedule: ZSchedule[Any, Option[List[Int]], Unit] =
      Schedule
        .spaced(5.seconds)
        .whileInput { data: Option[List[Int]] =>
          data match {
            case None => true
            case Some(data) =>
              data.size <= 100
          }
        }
        .andThen(secondSchedule)
        .unit

    def secondSchedule: ZSchedule[Any, Option[List[Int]], Unit] =
      Schedule
        .spaced(250.millis)
        .whileInput { data: Option[List[Int]] =>
          data match {
            case None => false
            case Some(data) =>
              data.size > 100
          }
        }
        .andThen(firstSchedule)
        .unit

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
            Schedule.recurs(2) && Schedule.spaced(1.seconds)
          ) *> ZIO.effect(done())
      )
  }
  // ZStream
  //   .repeatEffectWith(
  //     putStrLn("Hello Scala World 2019!"),
  //     Schedule.spaced(1.second)
  //   )
  //   .take(3)
  //   .runDrain
  //   .as(0)

}
