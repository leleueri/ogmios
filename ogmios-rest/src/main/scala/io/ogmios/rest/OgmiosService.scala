package io.ogmios.rest

import akka.actor.Actor
import akka.pattern._
import spray.routing._
import spray.http._
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
  implicit val timeout = Timeout(2.second)
  implicit val PersonFormat = jsonFormat4(Provider)
  
  def cassandraEndPoint: ActorRef
  
  val ogmiosRoute =
    path("providers"/Segment) { providerId =>
      put {
        entity(as[Provider]) { provider =>
          
          complete{
              val res = ask (cassandraEndPoint, new Register[Provider](provider)).mapTo[Status]
              Await.ready(res, 5.second)
              res.value match {
                case Some(s: Success[OpCompleted]) => StatusCodes.Created
                case _ => StatusCodes.InternalServerError
            }
          }
        }
      }
    }
}