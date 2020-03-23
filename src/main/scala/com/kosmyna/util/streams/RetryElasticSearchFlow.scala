package com.kosmyna.util.streams

import com.kosmyna.util.streams.AsyncRetryFlow.{ RetryException, WithRetries, WorkFn }
import com.sksamuel.elastic4s.ElasticDsl.{ bulk, _ }
import com.sksamuel.elastic4s.http.JavaClientExceptionWrapper
import com.sksamuel.elastic4s.requests.bulk.{ BulkCompatibleRequest, BulkResponseItem }
import com.sksamuel.elastic4s.{ ElasticClient, RequestFailure, RequestSuccess }
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.Future
import scala.concurrent.duration._

object RetryElasticSearchFlow extends LazyLogging {

	private def indexEs(client: ElasticClient)(batch: Seq[BulkCompatibleRequest]): Future[WithRetries[Seq[BulkResponseItem], Seq[BulkCompatibleRequest]]] = {
		try {
			val r = client.execute {
				bulk(batch)
			}
				.map {
					case RequestSuccess(status, body, headers, result) =>
						val (errors, success) = batch.zip(result.items)
							.partition {
								case (_, resp) => {
									resp.error.isDefined
								}
							}

						// Log out unique error messages
						val uniqueErrorMesgs = errors.flatMap {
							case (req, resp) => resp.error
						}.toSet
						uniqueErrorMesgs.foreach(e => logger.error(s"Es Result has failure: $e"))

						// Separate errors into ones that are actual failures and ones to retry
						val (retries, failures) = errors.partition {
							case (req, resp) =>
								// something happened at the bulkItem level (ex. a single request was too big, or the server got overloaded, or a bad request)
								// @todo: Only some errors are retryable, right now we retry nothing
								false
						}

						//  Collect the retry requests into a seq or None
						val retryRequests = if (retries.isEmpty) {
							None
						} else {
							Option(retries.map { case (req, resp) => req })
						}

						// Collect the finished Responses into a seq or None
						val finished = success ++ failures
						val finishedResp = if (finished.isEmpty) {
							None
						} else {
							Option(finished.map { case (req, resp) => resp })
						}

						WithRetries(finishedResp, retryRequests)

					// the request failed in elasticsearch
					case RequestFailure(status, body, headers, error) => {
						//						logger.error(s"Skipping, Failed es call $status, $body, $headers", error.asException)
						// Elasticsearch failed at the request level probably nothing to retry
						// example sending bad parameters to the endpoint / cant parse input parameters

						// SKIP THIS WHOLE BATCH - throw anything but a RetryException
						throw error.asException
					}

				}(scala.concurrent.ExecutionContext.global) // This should be a quick operation we can just use the global ec
				// something happened at the request level (ex. the socket connection was terminated)
				.recover {
					case JavaClientExceptionWrapper(wrappedException) => {
						wrappedException match {
							case h: java.io.IOError => {
								// RETRY THE WHOLE BATCH
								throw new RetryException("esClient IOError", h)
							}
							case i: java.io.IOException => {
								// RETRY THE WHOLE BATCH
								throw new RetryException("esClient IOException", i)
							}
						}
					}
				}(scala.concurrent.ExecutionContext.global) // This should be a quick operation we can just use the global ec
			r
		} catch { // The request failed before even getting to elasticsearch (ex. couldn't create socket connection)
			case e: java.io.IOException => {
				// RETRY THE WHOLE BATCH
				throw new RetryException("client.execute error", e)
			}
		}

	}

	def apply(
		client: ElasticClient,
		silencePeriod: FiniteDuration = 2.seconds
	): AsyncRetryFlow[Seq[BulkCompatibleRequest], Seq[BulkResponseItem]] = {
		val esFn: WorkFn[Seq[BulkCompatibleRequest], Seq[BulkResponseItem]] = indexEs(client)
		new AsyncRetryFlow[Seq[BulkCompatibleRequest], Seq[BulkResponseItem]](esFn)
	}

}
