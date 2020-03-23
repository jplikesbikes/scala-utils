package com.kosmyna.util.streams

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.scalatest.{ BeforeAndAfterAll, Suite }

import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * Extend this trait to add an implicit ActorSystem and Materializer
 * that gets automatically shutdown once all tests have finished
 */
trait AkkaStreamsFixture extends BeforeAndAfterAll { this: Suite =>

	implicit val system: ActorSystem = ActorSystem(getClass.getSimpleName)
	implicit val mat: ActorMaterializer = ActorMaterializer.create(system)

	override def afterAll(): Unit = {
		mat.shutdown()
		Await.result(system.terminate(), 5.seconds)
	}

}
