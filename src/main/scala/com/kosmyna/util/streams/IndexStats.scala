package com.kosmyna.util.streams

import akka.stream.scaladsl.Sink
import com.sksamuel.elastic4s.requests.bulk.BulkResponseItem

import scala.collection.mutable
import scala.concurrent.Future

object IndexStats {

	def prettyPrint(indexStats: mutable.Map[String, IndexerResponseStats]): String =
		indexStats.map { case (index, stats) => s"$index: $stats" }.mkString("\n")

	//Keep this mutable and reuse this object
	class IndexerResponseStats(success: Long = 0, failure: Long = 0) {
		var successes: Long = success
		var failures: Long = failure

		override def toString: String = s"IndexerResponseStats(successes=$successes, failures=$failures)"
	}

	/**
	 * Counts success and failures from indexing to ElasticSearch
	 */
	def countIndexerStats(): Sink[Seq[BulkResponseItem], Future[IndexerResponseStats]] =
		Sink.fold[IndexerResponseStats, Seq[BulkResponseItem]](
			new IndexerResponseStats(0, 0)
		)((acc, bulkResponseItems: Seq[BulkResponseItem]) => {
				bulkResponseItems.foreach(bulkResponseItem =>
					if (bulkResponseItem.error.isDefined) {
						acc.failures += 1
					} else {
						acc.successes += 1
					})
				acc
			})

	/**
	 * Counts success and failures from indexing to ElasticSearch per index
	 */
	def perIndexStats(): Sink[Seq[BulkResponseItem], Future[mutable.Map[String, IndexerResponseStats]]] = Sink.fold[mutable.Map[String, IndexerResponseStats], Seq[BulkResponseItem]](
		mutable.Map[String, IndexerResponseStats]()
	)(
			(acc, bulkResponseItems: Seq[BulkResponseItem]) => {
				bulkResponseItems.foreach(bulkResponseItem => {
					val stats = acc.getOrElseUpdate(bulkResponseItem.index, new IndexerResponseStats())
					if (bulkResponseItem.error.isDefined) {
						stats.failures += 1
					} else {
						stats.successes += 1
					}
				})
				acc
			}
		)

}
