package com.kosmyna.util.streams

import java.util.concurrent.atomic.AtomicReference

import akka.Done
import akka.stream.stage._
import akka.stream.{ Attributes, FlowShape, Inlet, Outlet }
import com.kosmyna.util.FutureUtils
import com.kosmyna.util.streams.AsyncRetryFlow.{ FormatInputFn, RetryException, WithRetries, WorkFn }
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

object AsyncRetryFlow extends LazyLogging {

	case class WithRetries[B, A](result: Option[B], retry: Option[A])
	type WorkFn[A, B] = A => Future[WithRetries[B, A]]
	type FormatInputFn[A] = A => String

	class RetryException(message: String) extends Exception(message) {

		def this(message: String, cause: Throwable) {
			this(message)
			initCause(cause)
		}

		def this(cause: Throwable) {
			this(Option(cause).map(_.toString).orNull, cause)
		}

	}

	def apply[A, B](workFn: WorkFn[A, B], silencePeriod: FiniteDuration): AsyncRetryFlow[A, B] =
		new AsyncRetryFlow(workFn, silencePeriod)

}

class AsyncRetryFlow[A, B](
	workFn: WorkFn[A, B],
	silencePeriod: FiniteDuration = 2.seconds,
	formatInput: FormatInputFn[A] = (a: A) => a.hashCode().toString
) extends GraphStage[FlowShape[A, B]] with LazyLogging {

	private val in = Inlet[A](s"AsyncRetryFlow.in")
	private val out = Outlet[B](s"AsyncRetryFlow.out")
	override def shape: FlowShape[A, B] = FlowShape.of(in, out)

	override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
		new TimerGraphStageLogic(shape) {

			// all calls to stages from within async functions need to go through AsyncCallback
			// the getAsyncCallback method can not be called from the constructor
			var completeCb: AsyncCallback[Unit] = _
			var pushCb: AsyncCallback[(A, B)] = _
			var pullCb: AsyncCallback[Option[A]] = _
			var scheduleCb: AsyncCallback[A] = _

			override def preStart(): Unit = {
				// Create all our async stage calls here
				completeCb = getAsyncCallback[Unit]((_) => {
					logger.info(s"completing stage")
					completeStage()
				})
				pushCb = getAsyncCallback[(A, B)] {
					case (elem, result) => {
						logger.debug(s"pushing result of ${formatInput(elem)}")
						push(out, result)
					}
				}
				pullCb = getAsyncCallback[Option[A]](elem => {
					val p = elem.map(formatInput).getOrElse("")
					logger.warn(s"skipping elem ${p}, getting next")
					if (!isClosed(in)) {
						pull(in)
					}
				})
				scheduleCb = getAsyncCallback[A](elem => {
					logger.warn(s"failed! waiting ${silencePeriod}, to rerun ${formatInput(elem)}")
					scheduleOnce(elem, silencePeriod)
				})
			}

			// We track two bits of state the current timer key if there is one and the current running workFn
			private val timerKey: AtomicReference[Option[A]] = new AtomicReference(None)
			private val workingFut: AtomicReference[Future[Done]] = new AtomicReference(Future.successful(Done))

			// Our main logic is kicked off by a new upstream element or a scheduled retry
			private def doRun(elem: A): Unit = {
				logger.trace(s"working on ${formatInput(elem)}")
				val working = workFn(elem)
					.flatMap(result => {
						val WithRetries(finished, retry) = result
						logger.trace(s"GOING TO: pushing result of ${formatInput(elem)}")

						val pushFut = finished.map(f => pushCb.invokeWithFeedback((elem, f))).getOrElse(Future.successful(Done))
						val retryFut = retry
							.map(r => {
								timerKey.set(Option(r))
								scheduleCb.invokeWithFeedback(r)
							})
							.getOrElse(Future.successful(Done))
						FutureUtils.settleWithTry(Seq(pushFut, retryFut)).map(_ => Done)
					})
					.recoverWith {
						case e: RetryException =>
							logger.trace(s"GOING TO: failed! waiting ${silencePeriod}, to rerun ${formatInput(elem)}")
							timerKey.set(Option(elem))
							scheduleCb.invokeWithFeedback(elem)
						case e: Throwable =>
							logger.trace(s"GOING TO: skipping elem ${formatInput(elem)}, getting next")
							pullCb.invokeWithFeedback(Option(elem))
					}
				workingFut.set(working)
			}

			/**
			 * We need to wait for any currently running work to finish and active timers to finish before completing
			 */
			private def doFinishWhenComplete() = {
				workingFut.get().onComplete {
					case _ =>
						logger.debug("workingFut complete")
						// are we waiting for a timer
						timerKey.get() match {
							case None =>
								logger.debug("no timers, complete stage")
								completeCb.invoke(())
							case Some(elem) => if (!isTimerActive(elem)) {
								logger.debug(s"timer ${formatInput(elem)} not active, complete stage")
								completeCb.invoke(())
							} else {
								logger.debug(s"timer ${formatInput(elem)} active, wait")
							}
						}
				}
			}

			// Handle inputstream events
			setHandler(in, new InHandler {
				override def onPush(): Unit = {
					val elem = grab(in)
					logger.debug(s"got from upstream ${formatInput(elem)}")
					doRun(elem)
				}

				// When the upstream finishes we need to wait for any currently running work to finish and active timers to finish
				override def onUpstreamFinish(): Unit = {
					val timerKeyStr = timerKey.get().map(formatInput).getOrElse("")
					logger.debug(s"upStream finished, waiting on workingFut: ${workingFut}, timer: ${timerKeyStr}")
					doFinishWhenComplete()
				}
			})

			// Handle outputstream events
			setHandler(out, new OutHandler {
				// When downstream asks for a new element
				override def onPull(): Unit = {
					val timerKeyStr = timerKey.get().map(formatInput).getOrElse("")
					logger.trace(s"pull request, ${workingFut.get()}, ${timerKeyStr}")
					// The upstream might not have any elements left, but we might be waiting on a work or a timer to finish
					if (!isClosed(in) && !hasBeenPulled(in)) pull(in)
				}

				// We can shutdown immediately and ignore any current timer or running work because the downstream doesn't need the results
				override def onDownstreamFinish(): Unit = {
					val timerKeyStr = timerKey.get().map(formatInput).getOrElse("")
					logger.debug(s"downstream finished")
					super.onDownstreamFinish()
				}
			})

			// handle timer events
			override protected def onTimer(timerKey: Any): Unit = {
				val elem = timerKey.asInstanceOf[A]
				logger.trace(s"got from retry ${formatInput(elem)}")
				doRun(elem)
				// Stop if upStream is closed
				if (isClosed(in)) {
					logger.trace("timer fired, in is closed. so completing stage")
					doFinishWhenComplete()
				}
			}

		}

}
