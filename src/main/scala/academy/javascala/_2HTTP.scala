package academy.javascala

import academy.javascala.JsonProtocol.WrappedInt
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.common.{EntityStreamingSupport, JsonEntityStreamingSupport}
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.server.Directives.path
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.{Sink, Source}
import akka.http.scaladsl.server.Directives._
import akka.stream.Materializer
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Random, Success}

object _2HTTP extends App {
  implicit val system = ActorSystem(Behaviors.empty, "SingleRequest")
  // needed for the future flatMap/onComplete in the end
  implicit val executionContext = system.executionContext

  val responseFuture: Future[HttpResponse] = Http().singleRequest(HttpRequest(uri = "http://127.0.0.1:8080/users"))

  responseFuture
    .onComplete {
      case Success(res) => println(res.entity.getDataBytes().map(_.utf8String).to(Sink.foreach(println)).run(Materializer(system)))
      case Failure(_) => sys.error("something wrong")
    }
}

object StreamingEndpoint extends App {
  private def startHttpServer(routes: Route)(implicit system: ActorSystem[_]): Unit = {
    // Akka HTTP still needs a classic ActorSystem to start
    import system.executionContext

    val futureBinding = Http().newServerAt("localhost", 8080).bind(routes)
    futureBinding.onComplete {
      case Success(binding) =>
        val address = binding.localAddress
        system.log.info("Server online at http://{}:{}/", address.getHostString, address.getPort)
      case Failure(ex) =>
        system.log.error("Failed to bind HTTP endpoint, terminating system", ex)
        system.terminate()
    }
  }

  implicit val system = ActorSystem(Behaviors.empty, "stream")

  import JsonProtocol._

  implicit val jsonStreamingSupport: JsonEntityStreamingSupport = EntityStreamingSupport.json()

  import akka.http.scaladsl.marshalling.PredefinedToEntityMarshallers._

  // https://doc.akka.io/docs/akka-http/10.0/common/marshalling.html
  def routes = path("stream") {
    val source = Source.tick(1.second, 5.seconds, "").map(_ => Random.nextInt(1000)).map(WrappedInt)
    complete(source)
  } ~ path("hello") {
    complete("world")
  }

  startHttpServer(routes)
}

object JsonProtocol
  extends akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
    with spray.json.DefaultJsonProtocol {

  case class WrappedInt(int: Int)

  implicit val tweetFormat: RootJsonFormat[WrappedInt] = jsonFormat1(WrappedInt.apply)
}
