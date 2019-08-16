package pv.view

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.Directives.{complete, get, path, _}
import akka.stream.ActorMaterializer
import org.apache.spark.sql.SparkSession
import pv.view.config.ConfigReader

import scala.io.StdIn

object ViewApp extends App
  with ViewProcess
  with ConfigReader {

  implicit val system = ActorSystem("view-app")
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  val config = readConfig.unsafeRunSync()

  implicit val sparkSession = SparkSession
    .builder()
    .config("spark.master", "local")
    .getOrCreate()

  val route =
    path("query") {
      get {
        complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, buildView(config)))
      }
    }

  val bindingFuture = Http().bindAndHandle(route, "localhost", 9999)

  println(s"Server online at http://localhost:9999/\nPress RETURN to stop...")
  StdIn.readLine() // let it run until user presses return
  bindingFuture
    .flatMap(_.unbind()) // trigger unbinding from the port
    .onComplete(_ => system.terminate()) // and shutdown when done
}
