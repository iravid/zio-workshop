package dev.zio.workshop

import zio._
import zio.stream._
import zio.console._
import zio.duration._

object Workshop extends App {
  def run(args: List[String]): ZIO[Environment, Nothing, Int] =
    ZStream
      .repeatEffectWith(
        putStrLn("Hello Scala World 2019!"),
        Schedule.spaced(1.second)
      )
      .take(3)
      .runDrain
      .as(0)
}
