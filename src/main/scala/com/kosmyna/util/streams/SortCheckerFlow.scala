package com.kosmyna.util.streams

import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler }
import akka.stream.{ Attributes, FlowShape, Inlet, Outlet }
import com.typesafe.scalalogging.LazyLogging

import scala.math.Ordering

object SortCheckerFlow {
	def apply[T, K: Ordering](fn: T => K, message: String = "") = new SortCheckerFlow[T, K](fn, message)
}

/**
 *  The input must be sorted by the key
 */
class SortCheckerFlow[T, K: Ordering](
	keyFn: T => K,
	message: String = "",
) extends GraphStage[FlowShape[T, T]] with LazyLogging {
	private[this] val in = Inlet[T]("SortChecker.in")
	private[this] val out = Outlet[T]("SortChecker.out")
	override val shape: FlowShape[T, T] = FlowShape(in, out)

	override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
		new GraphStageLogic(shape) with InHandler with OutHandler {
			import Ordering.Implicits._
			var noBuffered = true
			var previous: K = _

			override def onPush(): Unit = {
				val next = grab(in)
				val nextKey = keyFn(next)
				if (noBuffered) {
					previous = nextKey
					noBuffered = false
				}
				if (previous > nextKey) {
					throw new IllegalStateException(s"Out of order $message")
				}
				previous = nextKey
				push(out, next)
			}

			override def onPull(): Unit = pull(in)

			setHandlers(in, out, this)
		}

}
