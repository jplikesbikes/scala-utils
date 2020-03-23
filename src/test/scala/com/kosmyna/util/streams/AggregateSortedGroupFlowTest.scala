package com.kosmyna.util.streams

import akka.stream.scaladsl.{ Sink, Source }
import org.scalatest.AsyncFunSpec

class AggregateSortedGroupFlowTest extends AsyncFunSpec with AkkaStreamsFixture {

	def keyFn(item: (String, Int)): String = item._1
	def newAccFn(acc: Option[Int]): Int = 0
	def reduceFn(acc: Int, next: (String, Int)): Int = acc + next._2

	it("works on empty") {
		Source(List.empty[(String, Int)])
			.via(AggregateSortedGroupFlow[(String, Int), String, Int](keyFn, newAccFn, reduceFn))
			.runWith(Sink.seq)
			.map(actual => assert(actual === Seq.empty))
	}

	it("works on single same key") {
		Source(List(
			("a", 3),
		))
			.via(AggregateSortedGroupFlow[(String, Int), String, Int](keyFn, newAccFn, reduceFn))
			.runWith(Sink.seq)
			.map(actual => assert(actual === Seq(3)))
	}

	it("works on many same key") {
		Source(List(
			("a", 2),
			("a", 4),
		))
			.via(AggregateSortedGroupFlow[(String, Int), String, Int](keyFn, newAccFn, reduceFn))
			.runWith(Sink.seq)
			.map(actual => assert(actual === Seq(6)))
	}


	it("works on many to single") {
		Source(List(
			("a", 2),
			("b", 4),
		))
			.via(AggregateSortedGroupFlow[(String, Int), String, Int](keyFn, newAccFn, reduceFn))
			.runWith(Sink.seq)
			.map(actual => assert(actual === Seq(2, 4)))
	}

	it("works on many to many") {
		Source(List(
			("a", 2),
			("a", 3),
			("b", 4),
			("b", 3),
		))
			.via(AggregateSortedGroupFlow[(String, Int), String, Int](keyFn, newAccFn, reduceFn))
			.runWith(Sink.seq)
			.map(actual => assert(actual === Seq(5, 7)))
	}

	it("works when we stop consuming") {
	  Source(List(
		("a", 2),
		("a", 3),
		("b", 4),
		("b", 3),
	  ))
		  .via(AggregateSortedGroupFlow[(String, Int), String, Int](keyFn, newAccFn, reduceFn))
    	  .take(1)
		  .runWith(Sink.seq)
		  .map(actual => assert(actual === Seq(5)))
	}



}
