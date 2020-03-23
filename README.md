# scala-utils

A collection of some akka tools I use quite frequently in my dataflows

## AggregateSortedGroupFlow
Take a sorted stream as input and while the key is the same accumulate a value for the group. When the key changes output the final result of the accumulator applying an optional transform
```scala
// Count groups of sorted things
import com.kosmyna.util.streams.AggregateSortedGroupFlow
import akka.stream.scaladsl.{ Sink, Source }

def keyFn(item: (String, Int)): String = item._1
def newAccFn(acc: Option[Int]): Int = 0
def reduceFn(acc: Int, next: (String, Int)): Int = acc + next._2

val r = Source(List(("a", 3), ("a", 2), ("b", 1)))
    .via(AggregateSortedGroupFlow[(String, Int), String, Int](keyFn, newAccFn, reduceFn))
    .runWith(Sink.seq)
// r == Future{ Seq(5, 1) }
```

## ThroughputLoggerFlow
Log on a timer how many messages have passed through this flow
```scala
import akka.stream.DelayOverflowStrategy
import akka.stream.scaladsl.{ Sink, Source }
import com.kosmyna.util.streams.ThroughputLoggerFlow

import scala.concurrent.duration._

Source(Stream.from(1))
    .delay(20.millis, DelayOverflowStrategy.backpressure)
    .via(ThroughputLoggerFlow(40.millis))
    .take(20)
    .runWith(Sink.ignore)
```

## AsyncRetryFlow
Flow that makes it easy to retry async calls like things going over a network
```scala
import akka.stream.scaladsl.{ Sink, Source }
import com.kosmyna.util.streams.AsyncRetryFlow
import scala.concurrent.Future
import scala.concurrent.duration._

// this should throw a RetryException anytime it wants to be retried
def replaceWithSomethingLikeHttpRequests(value: Int): Future[Int] = Future{ value }

val reponsesThatWereRetriedOnFailures = Source.apply(List(1, 2, 0, 3, 4))
    .via(AsyncRetryFlow[Int, Int](replaceWithSomethingLikeHttpRequests, 1.minute))
    .runWith(Sink.collection)
```

## RetryElasticSearchSink
The implementation uses AsyncRetryFlow
Simple flow that will retry on network errors, and because es times out when overloaded this can act as a dumb throttle
```scala
import akka.stream.scaladsl.Flow
import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.requests.bulk.BulkCompatibleRequest
import com.kosmyna.util.streams.RetryElasticSearchFlow
import com.kosmyna.util.streams.IndexStats

val esClient: ElasticClient = ???
val indexStats = Flow[Seq[BulkCompatibleRequest]]
    .via(RetryElasticSearchFlow(esClient))
    .runWith(IndexStats.perIndexStats())
```

## SearchCheckerFlow
A simple flow that will throw an exception if the data running through it isn't sorted
```scala
import akka.stream.scaladsl.{ Sink, Source }
import com.kosmyna.util.streams.SortCheckerFlow

// this will throw an exception
val result = Source(List(1, 2, 3, 5, 4))
    .via(SortCheckerFlow(identity))
    .runWith(Sink.ignore)
```
