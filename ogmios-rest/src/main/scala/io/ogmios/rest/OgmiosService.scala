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
import org.ogmios.core.action.Register
import akka.util.Timeout
import scala.concurrent.duration._
import org.ogmios.core.bean.Status
import scala.concurrent.Await
import scala.util.Success
import org.ogmios.core.bean.OpCompleted
import org.ogmios.core.bean.OpCompleted
import org.ogmios.core.action.Read
import org.ogmios.core.bean.OpResult
import org.ogmios.core.bean.OpFailed
import org.ogmios.core.bean.OpFailed
import org.ogmios.core.bean.OpFailed
import org.ogmios.core.bean.OpCompleted
import io.ogmios.rest.exception.NotFoundException
import org.omg.CosNaming.NamingContextPackage.NotFound
import org.ogmios.core.bean.OpFailed
import io.ogmios.rest.exception.OgmiosException
import io.ogmios.rest.exception.OgmiosException
import io.ogmios.rest.exception.InternalErrorException
import io.ogmios.rest.exception.NotFoundException

// we don't implement our route structure directly in the service actor because
// we want to be able to test it independently, without having to spin up an actor
class OgmiosServiceActor extends Actor with OgmiosService with ActorNames {

  // the HttpService trait defines only one abstract member, which
  // connects the services environment to the enclosing actor or test
  def actorRefFactory = context

  // this actor only runs our route, but you could add
  // other things here, like request stream processing
  // or timeout handling
  def receive = runRoute(ogmiosRoute)

  // create and start cassandra actor
  override val cassandraEndPoint = context.actorOf(Props[CassandraActor], cassandraActor)
}

// this trait defines our service behavior independently from the service actor
trait OgmiosService extends HttpService  {
  
  // Await timeout
  implicit val timeout = Timeout(2.second)
  // JSON unmarshaller
  implicit val providerJsonFormat = jsonFormat4(Provider)
  implicit val opFailedJsonFormat = jsonFormat2(OpFailed)
  
  def cassandraEndPoint: ActorRef
  
  /**
   * This handler complete the route with a status adapted to the received exception
   * Unmanaged exception are computed by the default spray handler 
   */
  implicit val ogmiosExceptionHandler = ExceptionHandler {
    case ex : OgmiosException => complete(ex.status, ex.opStatus)
  }
  
  val ogmiosRoute =
    path("providers"/Segment) { providerId =>
      put {
        entity(as[Provider]) { provider =>
          // Are there a better way to manage Future response?
          val res = Await.result(ask (cassandraEndPoint, new Register[Provider](provider)).mapTo[Status], 5.second)
          res match {
            case OpCompleted(_,_) => complete(Created)
            case op : OpFailed => throw new InternalErrorException(op)
          }
        }
      } ~
      get {
        complete {
          val res = Await.result(ask (cassandraEndPoint, new Read[Provider](providerId)).mapTo[Status], 5.second)
          res match {
            case OpResult(_, _, value: Provider) => value
            case op : OpFailed => throw if (op.state == Status.StateNotFound ) 
              new NotFoundException(op) 
            else 
              new InternalErrorException(op)
          }
        }
      }
    }
}
