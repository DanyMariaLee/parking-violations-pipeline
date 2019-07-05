package pv.http.service

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives.{as, complete, entity, post}
import akka.kafka.ProducerSettings
import akka.stream.ActorMaterializer
import org.apache.kafka.common.serialization.StringSerializer
import pv.common.json.JsonProtocols
import pv.common.output.domain.WrappedNYCData
import pv.http.service.config.ConfigReader
import pv.http.service.publish.PublishService
import pv.http.service.serialize.KafkaJsonSerializer

import scala.io.StdIn

object HttpService extends App
  with PublishService
  with ConfigReader
  with JsonProtocols {

  implicit val system = ActorSystem("parking-violation-http-service")
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  val config = readConfig.unsafeRunSync()

  implicit val producerSettings = ProducerSettings(
    system,
    new StringSerializer,
    new KafkaJsonSerializer
  )
    .withBootstrapServers(config.bootstrapServers)

  val route = post {
    entity(as[WrappedNYCData]) { nycData =>
      complete(publish(nycData, config.topic))
    }
  }

  val bindingFuture = Http()
    .bindAndHandle(route, config.host, config.port)

  println(s"Recipient server online at http://${config.host}:${config.port}/" +
    "\nPress RETURN to stop...\n")
  StdIn.readLine()
  bindingFuture
    .flatMap(_.unbind())
    .onComplete(_ => system.terminate())

}
