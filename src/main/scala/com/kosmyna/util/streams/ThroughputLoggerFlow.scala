package com.kosmyna.util.streams

import akka.stream.stage._
import akka.stream.{ Attributes, FlowShape, Inlet, Outlet }
import com.kosmyna.util.streams.ThroughputLoggerFlow.LogFormatter
import com.typesafe.scalalogging.{ LazyLogging, Logger }

import scala.concurrent.duration.{ FiniteDuration, _ }

object ThroughputLoggerFlow extends LazyLogging {
	type LogFormatter = (Long, Long, Long, Long) => String

	def defaultFormatter(divideByMillis: Long, timePeriod: String)(start: Long, end: Long, count: Long, total: Long): String = {
		val periodTook = (end - start) / divideByMillis.toDouble
		val elementsPerperiod = "%.3f".format(count / periodTook)
		s"$count items in $periodTook ${timePeriod}s ($elementsPerperiod per $timePeriod) total: $total"
	}

	val perSecondFormat: LogFormatter = defaultFormatter(1000, "sec")
	val perMinuteFormat: LogFormatter = defaultFormatter(1000 * 60, "min")
	val perHourFormat: LogFormatter = defaultFormatter(1000 * 60 * 60, "hour")

	def makePrefixFormatFn(prefix: String, fn: LogFormatter): LogFormatter = (start: Long, end: Long, count: Long, total: Long) => {
	  	s"$prefix ${fn(start, end, count, total)}"
	}

	def apply[A](
		loggingPeriod: FiniteDuration = 1.minute,
		formatFn: LogFormatter = ThroughputLoggerFlow.perSecondFormat,
		logger: Logger = logger,
	): ThroughputLoggerFlow[A] =
		new ThroughputLoggerFlow[A](loggingPeriod, formatFn, mesg => logger.info(mesg))
}

class ThroughputLoggerFlow[A](
	loggingPeriod: FiniteDuration = 1.minute,
	formatFn: LogFormatter = ThroughputLoggerFlow.perSecondFormat,
	logFn: String => Unit = println
) extends GraphStage[FlowShape[A, A]] with LazyLogging {

	private val in = Inlet[A](s"${getClass.getName}.in")
	private val out = Outlet[A](s"${getClass.getName}.out")

	override def shape: FlowShape[A, A] = FlowShape.of(in, out)

	override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
		new TimerGraphStageLogic(shape) with OutHandler with InHandler {
			var start: Long = _
			var numRecords = 0L
			var totalRecords = 0L
			var currentStart: Long = _

		  	// Kick off the first timer event
			override def preStart(): Unit = {
				start = System.currentTimeMillis()
				scheduleOnce(start, loggingPeriod)
			}

			override def postStop(): Unit = {
				// Log out the last batch
				val endTime = System.currentTimeMillis()
				totalRecords += numRecords
				val logMesg = formatFn(currentStart, endTime, numRecords, totalRecords)
				logFn(s"last batch: $logMesg")

				// Log out the final summary
				val took = endTime - start
				val tookSeconds = "%.3f".format(took / 1000d)
				val tookMinutes = "%.3f".format(took / (1000d * 60))
				val tookHours = "%.3f".format(took / (1000d * 60 * 60))
				logFn(s"finished: $totalRecords items in $tookSeconds secs (or $tookMinutes mins) (or $tookHours hours)")
			}

			override def onPush(): Unit = {
				push(out, grab(in))
				numRecords += 1
			}

			override def onPull(): Unit = pull(in)

			// handle timer events
			override protected def onTimer(timerKey: Any): Unit = {
				val startTime = timerKey.asInstanceOf[Long]
				val endTime = System.currentTimeMillis()

				// add this batch to the total
				totalRecords += numRecords

				logFn(formatFn(startTime, endTime, numRecords, totalRecords))

				// reset the count and schedule the next log
				numRecords = 0
				currentStart = System.currentTimeMillis()
				scheduleOnce(currentStart, loggingPeriod)
			}

			setHandlers(in, out, this)
		}

}
