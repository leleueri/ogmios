package io.ogmios.service

import akka.actor.Actor
import akka.pattern._
import spray.routing._
import spray.http._
import spray.http.StatusCodes._
import MediaTypes._
import akka.actor.Props
import akka.actor.ActorRef
import spray.httpx.unmarshalling._
import spray.httpx.marshalling._
import spray.httpx.SprayJsonSupport._
import spray.json._
import DefaultJsonProtocol._
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.util.Success
import scala.concurrent.ExecutionContext.Implicits.global
import spray.routing.directives.LoggingMagnet
import akka.actor.ActorLogging
import org.slf4j.Logger
import akka.event.LoggingAdapter
import spray.http.StatusCodes._
import spray.http.StatusCode
import io.ogmios.exception._
import io.ogmios.core.action._
import io.ogmios.core.bean._
import io.ogmios.core.actor.CassandraActor
import io.ogmios.core.bean.Provider
import io.ogmios.core.actor.ActorNames
import com.datastax.driver.core.ResultSet
import scala.concurrent.Future
import java.util.Iterator
import scala.collection.JavaConversions._

// we don't implement our route structure directly in the service actor because
// we want to be able to test it independently, without having to spin up an actor
class OgmiosServiceActor extends Actor with OgmiosService with ActorNames with ActorLogging {

  // the HttpService trait defines only one abstract member, which
  // connects the services environment to the enclosing actor or test
  def actorRefFactory = context

  // this actor only runs our route, but you could add
  // other things here, like request stream processing
  // or timeout handling
  def receive = runRoute(ogmiosRoute)

  def getLogger = log
  
  // create and start cassandra actor
  // TODO manage the actor lifecycle (if cassandra crashed and restart the actor no request can be computed due to missing actor)
  override val cassandraEndPoint = context.actorOf(Props[CassandraActor], cassandraActor)
}

// this trait defines our service behavior independently from the service actor
trait OgmiosService extends HttpService {
  
  // ask timeout
  implicit val timeout = Timeout(2.second)
  // JSON unmarshaller
  implicit val providerJsonFormat = jsonFormat4(Provider)
  implicit val eventJsonFormat = jsonFormat4(Event)
  implicit val metricJsonFormat = jsonFormat4(Metric)
  implicit val opFailedJsonFormat = jsonFormat2(OpFailed)
  
  def cassandraEndPoint: ActorRef
  
  def getLogger: LoggingAdapter
  
  def printRequestMethod(req: HttpRequest): Unit = getLogger.info(req.toString)
  
  /**
   * This handler complete the route with a status adapted to the received exception
   * Unmanaged exception are computed by the default spray handler 
   */
  implicit val ogmiosExceptionHandler = ExceptionHandler {
    case ex : OgmiosException => complete(ex.status, ex.opStatus)
    case unexp : Exception => complete(InternalServerError, new OpFailed(OgmiosStatus.StateKo, s"Unexpected error : ${unexp.getMessage}"))
  }
  
  val ogmiosRoute = logRequest (LoggingMagnet(printRequestMethod)) {
    path("providers"/Segment) { providerId =>
      put {
        entity(as[Provider]) { provider =>
          if (provider.id == providerId) {
              val asyncResponse = ask (cassandraEndPoint, new Register[Provider](provider)).mapTo[OgmiosStatus].map(_ => Created).recover{case ex:Throwable => throw ex}
              complete(asyncResponse)
          } else {
            throw new InvalidArgumentException("Inconsistent provider identifier")
          }
        }
      } ~
      post {
        parameterMap { params =>
          if (params.contains("event") && params.contains("metric")) {
            throw new InvalidArgumentException("Event and metric can't be sent in the same request")
          } else if (params.contains("event")) {
            entity(as[Event]) { eventBean =>
              registerMessage(eventBean, providerId)
            }
          } else if (params.contains("metric")) {
            entity(as[Metric]) { metricBean =>
              registerMessage(metricBean, providerId)
            }
          } else {
            entity(as[Provider]) { provider =>
              if (provider.id == providerId) {
                  val asyncResponse = ask (cassandraEndPoint, new Update[Provider](provider)).mapTo[OgmiosStatus].map(_ => OK).recover{case ex:Throwable => throw ex}
                  complete(asyncResponse)
              } else {
                  throw new InvalidArgumentException("Inconsistent provider identifier")
              }
            }
          } 
        }
      } ~ 
      delete {
        complete {
          ask (cassandraEndPoint, new DeleteProvider(providerId))
          .mapTo[OgmiosStatus]
          .map(_ => NoContent)
          .recover{case ex:Throwable => throw ex}
        }
      } ~
      get {
        parameterMap { params =>
          if (params.contains("type") && !params.contains("name") && !params.contains("begin")) {
            throw new InvalidArgumentException("Timeline read required some parameter")
          } else if (params.contains("type") && params.get("type").get == "events") {
            // return a list of events 
            val name = params.get("name") match {
              case Some(str) => str
              case None =>  throw new InvalidArgumentException("Event name required")
            }
            val begin = params.get("begin") match {
              case Some(str) => str.toLong
              case None =>  throw new InvalidArgumentException("Event begin date required")
            }
            
            val resultSetFuture = ask (cassandraEndPoint, new ReadEventsTimeline(providerId, name, begin, params.get("end").map(_.toLong)))
            .mapTo[OpResult[Future[ResultSet]]].flatMap(_.value).recover{case ex:Throwable => throw ex}

            val res = resultSetFuture.map(rs => 
               for {
                 r <- rs.all()
               } yield new Event(r.getString("providerid"), r.getDate("generation").getTime, r.getString("eventname"), r.getMap("properties", classOf[String], classOf[String]).toMap)
            ).recover{case ex:Throwable => throw ex}
            
            complete(res.map(_.toList))
            
          } else if (params.contains("type") && params.get("type").get == "metrics") {
            // return a list of metrics
            val name = params.get("name") match {
              case Some(str) => str
              case None =>  throw new InvalidArgumentException("Event name required")
            }
            val begin = params.get("begin") match {
              case Some(str) => str.toLong
              case None =>  throw new InvalidArgumentException("Event begin date required")
            }
            
            val resultSetFuture = ask (cassandraEndPoint, new ReadMetricsTimeline(providerId, name, begin, params.get("end").map(_.toLong)))
            .mapTo[OpResult[Future[ResultSet]]].flatMap(_.value).recover{case ex:Throwable => throw ex}

            val res = resultSetFuture.map(rs => 
               for {
                 r <- rs.all()
               } yield new Metric(r.getString("providerid"), r.getDate("generation").getTime, r.getString("metricname"), r.getDouble("value"))
            ).recover{case ex:Throwable => throw ex}
            
            complete(res.map(_.toList))
          } else {
            // Get provider description
            complete( ask(cassandraEndPoint, new Read[Provider](providerId)).mapTo[OpResult[Provider]]
                  .map((resultat : OpResult[Provider]) => resultat.value).recover{case ex:Throwable => throw ex})
          } 
        }
      }
    }
  }
  
  def registerMessage[T <: Message](message: T, providerId: String) : Route = {
      if (message.provider == providerId) {
          val asyncResponse = ask (cassandraEndPoint, new Register[T](message)).mapTo[OgmiosStatus].map(_ => OK).recover{case ex:Throwable => throw ex}
          complete(asyncResponse)
      } else {
          throw new InvalidArgumentException("Inconsistent provider identifier")
      }
  }
}
