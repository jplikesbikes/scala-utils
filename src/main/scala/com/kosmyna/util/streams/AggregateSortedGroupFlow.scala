package com.kosmyna.util.streams

import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler }
import akka.stream.{ Attributes, FlowShape, Inlet, Outlet }
import com.typesafe.scalalogging.LazyLogging

object AggregateSortedGroupFlow {

	def apply[Input, Key, Acc](
		keyFn: Input => Key,
		newAccFn: Option[Acc] => Acc,
		reduceFn: (Acc, Input) => Acc,
	): AggregateSortedGroupFlow[Input, Key, Acc, Acc] = apply(keyFn, newAccFn, reduceFn, (_, a) => a)

	def apply[Input, Key, Acc, Output](
		keyFn: Input => Key,
		newAccFn: Option[Acc] => Acc,
		reduceFn: (Acc, Input) => Acc,
		completeReduce: (Key, Acc) => Output,
	): AggregateSortedGroupFlow[Input, Key, Acc, Output] = new AggregateSortedGroupFlow(keyFn, newAccFn, reduceFn, completeReduce)

}


/**
 *  The input must be sorted by the key
 *  you can force a this constraint to throw an exception by using
 *  @see com.kosmyna.util.streams.SortCheckerFlow
 */
class AggregateSortedGroupFlow[Input, Key, Acc, Output](
	keyFn: Input => Key, // Get the key from the input
	newAccFn: Option[Acc] => Acc, // Make a new accumulator from a possible previous one
	reduceFn: (Acc, Input) => Acc, // Do the reduce
	completeReduce: (Key, Acc) => Output // Convert the accumulator to the final output
) extends GraphStage[FlowShape[Input, Output]] with LazyLogging {
	private val in = Inlet[Input](s"AggregateSortedGroupFlow.in")
	private val out = Outlet[Output](s"AggregateSortedGroupFlow.out")
	override def shape: FlowShape[Input, Output] = FlowShape.of(in, out)

	override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {
		private var key: Key = _
		private var acc: Acc = _

		override def onPush(): Unit = {
			val elem = grab(in)

			val elemKey = keyFn(elem)
			if (key == null) {
				key = elemKey
				acc = newAccFn(None)
			}

			if (key == elemKey) {
				acc = reduceFn(acc, elem)
				pull(in)
			} else {
				val result = completeReduce(key, acc)
				key = elemKey
				acc = reduceFn(newAccFn(Option(acc)), elem)
				push(out, result)
			}
		}

		override def onUpstreamFinish(): Unit = {
			if (key != null && isAvailable(out)) {
				val result = completeReduce(key, acc)
				push(out, result)
			}
			if (key == null || isClosed(out)) {
				completeStage()
			}
		}

		override def onPull(): Unit = {
			if (!isClosed(in) && !hasBeenPulled(in)) {
				pull(in)
			} else {
				if (key == null) {
					val result = completeReduce(key, acc)
					push(out, result)
				}
				completeStage()
			}
		}

		setHandlers(in, out, this)
	}
}
