package com.kosmyna.util.streams

import akka.stream.scaladsl.{ Sink, Source }
import org.scalatest.AsyncFunSpec

import scala.concurrent.Await
import scala.concurrent.duration._

class SortCheckerFlowTest extends AsyncFunSpec with AkkaStreamsFixture {

	it("should pass sorted") {
		val input = List(1, 2, 3, 4, 5)

		val result = Source(input)
			.via(SortCheckerFlow(identity))
			.runWith(Sink.seq)

		result.map(actual => {
			assert(actual === input)
		})
	}

	it("should fail unsorted") {
		assertThrows[IllegalStateException] { // Result type: Assertion
			val input = List(1, 3, 2, 4, 5)

			val result = Source(input)
				.via(SortCheckerFlow(identity))
				.runWith(Sink.ignore)
			Await.result(result, 10.seconds)
		}
	}

}
