package io.ogmios.rest

import akka.actor.Actor
import akka.pattern._
import spray.routing._
import spray.http._
import spray.http.StatusCodes._
import MediaTypes._
import akka.actor.Props
import org.ogmios.core.actor.CassandraActor
import akka.actor.ActorRef
import org.ogmios.core.bean.Provider
import spray.httpx.unmarshalling._
import spray.httpx.marshalling._
import spray.httpx.SprayJsonSupport._
import spray.json._
import DefaultJsonProtocol._
import org.ogmios.core.actor.ActorNames
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.util.Success
import org.ogmios.core.action._
import org.ogmios.core.bean._
import io.ogmios.rest.exception._
import scala.concurrent.ExecutionContext.Implicits.global
import spray.routing.directives.LoggingMagnet
import akka.actor.ActorLogging
import org.slf4j.Logger
import akka.event.LoggingAdapter

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
  override val cassandraEndPoint = context.actorOf(Props[CassandraActor], cassandraActor)
}



// this trait defines our service behavior independently from the service actor
trait OgmiosService extends HttpService {
  
  // Await timeout
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
  }
  
  val ogmiosRoute = logRequest (LoggingMagnet(printRequestMethod)) {
    path("providers"/Segment) { providerId =>
      put {
        entity(as[Provider]) { provider =>
          if (provider.id == providerId) {
              val asyncResponse = ask (cassandraEndPoint, new Register[Provider](provider)).mapTo[Status].map(_ match {
                case OpCompleted(_,_) => Created
                case op : OpFailed if op.state == Status.StateConflict => throw new ConflictException(op)
                case op : OpFailed => throw new InternalErrorException(op)
              })
              complete(asyncResponse)
          } else {
            complete(BadRequest, new OpFailed(Status.StateKo, "Inconsistent providerID"))
          }
        }
      } ~
      post {
        parameterMap { params =>
          if (params.isDefinedAt("event")) {
            entity(as[Event]) { eventBean =>
              if (eventBean.provider == providerId) {
                  val asyncResponse = ask (cassandraEndPoint, new Register[Event](eventBean)).mapTo[Status].map(_ match {
                    case OpCompleted(_,_) => OK
                    case op : OpFailed => throw new InternalErrorException(op)
                  })
                  complete(asyncResponse)
              } else {
                complete(BadRequest, new OpFailed(Status.StateKo, "Inconsistent providerID"))
              }
            }
          } else if (params.isDefinedAt("metric")) {
            entity(as[Metric]) { metricBean =>
              if (metricBean.provider == providerId) {
                  val asyncResponse = ask (cassandraEndPoint, new Register[Metric](metricBean)).mapTo[Status].map(_ match {
                    case OpCompleted(_,_) => OK
                    case op : OpFailed => throw new InternalErrorException(op)
                  })
                  complete(asyncResponse)
              } else {
                complete(BadRequest, new OpFailed(Status.StateKo, "Inconsistent providerID"))
              }
            }
          } else {entity(as[Provider]) { provider =>
              if (provider.id == providerId) {
                  val asyncResponse = ask (cassandraEndPoint, new Update[Provider](provider)).mapTo[Status].map(_ match {
                    case OpCompleted(_,_) => OK
                    case op : OpFailed if op.state == Status.StateNotFound => throw new NotFoundException(op)
                    case op : OpFailed => throw new InternalErrorException(op)
                  })
                  complete(asyncResponse)
              } else {
                complete(BadRequest, new OpFailed(Status.StateKo, "Inconsistent providerID"))
              }
            }
          } 
        }
      }~
      get {
        complete {
          ask (cassandraEndPoint, new Read[Provider](providerId)).mapTo[Status].map(_ match {
            case OpResult(_, _, value: Provider) => value
            case op : OpFailed if op.state == Status.StateNotFound => throw new NotFoundException(op)
            case op : OpFailed => throw new InternalErrorException(op)
          })
        }
      }
    }
  }
}
