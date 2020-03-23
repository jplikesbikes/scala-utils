package com.kosmyna.util.streams

import java.util.concurrent.atomic.AtomicInteger

import akka.stream.DelayOverflowStrategy
import akka.stream.scaladsl.{ Sink, Source }
import com.kosmyna.util.streams.ThroughputLoggerFlowTest.CountingLogger
import org.scalatest.AsyncFunSpec

import scala.concurrent.duration._

object ThroughputLoggerFlowTest {
	class CountingLogger() {
		val ctr = new AtomicInteger()
		def logFn(start: Long, end: Long, count: Long, total: Long): String = {
			ctr.incrementAndGet()
			""
		}
	}
}

class ThroughputLoggerFlowTest extends AsyncFunSpec with AkkaStreamsFixture {

	it("should print at least once and not loose any event") {
		val countingLogger = new CountingLogger()

		val resultFut = Source(Stream.from(1))
			.delay(20.millis, DelayOverflowStrategy.backpressure)
			.via(ThroughputLoggerFlow(40.millis, countingLogger.logFn))
			.take(20)
			.runWith(Sink.seq)

		resultFut.map(actual => {
			assert(countingLogger.ctr.get() > 0)
			assert(actual == 1.to(20))
		})
	}

}
