package com.kosmyna.util.streams

import akka.NotUsed
import akka.stream.scaladsl.{ Flow, Sink, Source }
import com.kosmyna.util.streams.AsyncRetryFlow.{ RetryException, WithRetries }
import org.scalatest.AsyncFunSpec

import scala.concurrent.Future
import scala.concurrent.duration._

class AsyncRetryFlowTest extends AsyncFunSpec with AkkaStreamsFixture {

	def workFn(a: Int): Future[WithRetries[Int, Int]] = Future {
		val result = 10 / a
		WithRetries[Int, Int](Option(result), None)
	}

	def myFailFn(a: Int): Future[WithRetries[Int, Int]] = Future {
		WithRetries[Int, Int](None, Option(a))
	}

	def myFailFnWithException(a: Int): Future[WithRetries[Int, Int]] = Future.failed(new RetryException(""))

	def myFailFnWithRetries(a: Int): Future[WithRetries[Int, Int]] = Future {
		val result = 10 / a
		WithRetries[Int, Int](Option(result), Option(a * 10))
	}

	// Fail every third call
	def wrapWithFailures[A, B](fn: A => B, myFailFn: A => B): A => B = {
		var ctr = 0

		def failFn(a: A): B = {
			ctr += 1
			if (ctr % 3 == 0) {
				myFailFn(a)
			} else {
				fn(a)
			}
		}

		failFn
	}

	val retryTimeout: FiniteDuration = 50.millis

	it("can filter calls that fail every 3rd and in the middle") {
		val range = List(1, 2, 0, 3, 4)
		val source: Source[Int, NotUsed] = Source.apply(range)

		val wf = wrapWithFailures(workFn, myFailFn)
		val sink = Flow.fromGraph(new AsyncRetryFlow[Int, Int](wf, retryTimeout))

		val resultFut = source
			.via(Flow.fromGraph(sink))
			.runWith(Sink.collection)

		resultFut.map(result => {
			val expected = range.filter(_ != 0).map(10 / _)
			assert(expected == result)
		})
	}

	it("can filter calls that fail every 3rd and in the start") {
		val range = List(0, 1, 2, 3, 4)
		val source: Source[Int, NotUsed] = Source.apply(range)

		val wf = wrapWithFailures(workFn, myFailFn)
		val sink = Flow.fromGraph(new AsyncRetryFlow[Int, Int](wf, retryTimeout))

		val resultFut = source
			.via(Flow.fromGraph(sink))
			.runWith(Sink.collection)

		resultFut.map(result => {
			val expected = range.filter(_ != 0).map(10 / _)
			assert(expected == result)
		})
	}

	it("can filter calls that fail every 3rd and in the end") {
		val range = List(1, 2, 3, 4, 0)
		val source: Source[Int, NotUsed] = Source.apply(range)

		val wf = wrapWithFailures(workFn, myFailFn)
		val sink = Flow.fromGraph(new AsyncRetryFlow[Int, Int](wf, retryTimeout))

		val resultFut = source
			.via(Flow.fromGraph(sink))
			.runWith(Sink.collection)

		resultFut.map(result => {
			val expected = range.filter(_ != 0).map(10 / _)
			assert(expected == result)
		})
	}

	it("can filter when everything fails") {
		val range = List(0, 0, 0, 0, 0)
		val source: Source[Int, NotUsed] = Source.apply(range)

		val wf = wrapWithFailures(workFn, myFailFn)
		val sink = Flow.fromGraph(new AsyncRetryFlow[Int, Int](wf, retryTimeout))

		val resultFut = source
			.via(Flow.fromGraph(sink))
			.runWith(Sink.collection)

		resultFut.map(result => {
			val expected = Seq()
			assert(expected == result)
		})
	}

	it("can filter failures and end early") {
		val range = List(1, 2, 0, 3, 4)
		val source: Source[Int, NotUsed] = Source.apply(range)

		val wf = wrapWithFailures(workFn, myFailFn)
		val sink = Flow.fromGraph(new AsyncRetryFlow[Int, Int](wf, retryTimeout))

		val resultFut = source
			.via(Flow.fromGraph(sink))
			.take(4)
			.runWith(Sink.collection)

		resultFut.map(result => {
			val expected = range.filter(_ != 0).map(10 / _).take(4)
			assert(expected == result)
		})
	}

	it("can filter failures and end at the exact end") {
		val range = List(0, 1, 2, 3, 4)
		val source: Source[Int, NotUsed] = Source.apply(range)

		val wf = wrapWithFailures(workFn, myFailFn)
		val sink = Flow.fromGraph(new AsyncRetryFlow[Int, Int](wf, retryTimeout))

		val resultFut = source
			.via(Flow.fromGraph(sink))
			.take(4)
			.runWith(Sink.collection)

		resultFut.map(result => {
			val expected = range.filter(_ != 0).map(10 / _).take(4)
			assert(expected == result)
		})
	}

	it("can filter failures and end early on a failure") {
		val range = List(1, 2, 3, 0, 4)
		val source: Source[Int, NotUsed] = Source.apply(range)

		val wf = wrapWithFailures(workFn, myFailFn)
		val sink = Flow.fromGraph(new AsyncRetryFlow[Int, Int](wf, retryTimeout))

		val resultFut = source
			.via(Flow.fromGraph(sink))
			.take(4)
			.runWith(Sink.collection)

		resultFut.map(result => {
			val expected = range.filter(_ != 0).map(10 / _).take(4)
			assert(expected == result)
		})
	}

	it("can end early with all failures") {
		val range = List(0, 0, 0, 0, 0)
		val source: Source[Int, NotUsed] = Source.apply(range)

		val wf = wrapWithFailures(workFn, myFailFn)
		val sink = Flow.fromGraph(new AsyncRetryFlow[Int, Int](wf, retryTimeout))

		val resultFut = source
			.via(Flow.fromGraph(sink))
			.take(4)
			.runWith(Sink.collection)

		resultFut.map(result => {
			val expected = Seq()
			assert(expected == result)
		})
	}

	it("can perform retries") {
		val range = List(0, 1, 2, 3, 4)
		val source: Source[Int, NotUsed] = Source.apply(range)

		val wf = wrapWithFailures(workFn, myFailFnWithRetries)
		val sink = Flow.fromGraph(new AsyncRetryFlow[Int, Int](wf, retryTimeout))

		val resultFut = source
			.via(Flow.fromGraph(sink))
			.runWith(Sink.collection)

		resultFut.map(result => {
			val expected = Seq(1, 2, 3, 4, 20, 200).map(10 / _)
			assert(expected == result)
		})
	}

}
