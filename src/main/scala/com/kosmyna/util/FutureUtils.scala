package com.kosmyna.util

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

object FutureUtils {

	def allEithers[L <: Throwable, R](eithers: Seq[Either[L, R]])(implicit ec: ExecutionContext): Future[Seq[R]] =
		Future.sequence(eithers.map(_.fold(Future.failed, Future.successful)))

	def recoverFutureWithTry[T](future: Future[T])(implicit ec: ExecutionContext): Future[Try[T]] =
		future.map(Success(_)).recover { case x => Failure[T](x) }

	def settleWithTry[T](futures: Seq[Future[T]])(implicit ec: ExecutionContext): Future[Seq[Try[T]]] =
		Future.sequence(futures.map(recoverFutureWithTry))

	def liftFromEither[E, T](either: Either[E, Future[T]])(implicit ec: ExecutionContext): Future[Either[E, T]] =
		either.fold(
			left => Future(Left(left)),
			right => right.map(t => Right(t))
		)

}
