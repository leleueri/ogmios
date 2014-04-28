package org.ogmios.core.actor

import akka.actor.{ActorLogging, Actor}
import akka.actor.Actor.Receive
import org.ogmios.core.action.Register
import org.ogmios.core.bean.Provider
import akka.actor.Props
import org.ogmios.core.action.RegisterFrom
import org.ogmios.core.config.CassandraCluster
import org.ogmios.core.config.ConfigCassandraCluster
import akka.actor.ActorRef
import scala.concurrent.Future
import com.datastax.driver.core.ResultSet
import scala.util.Success
import scala.util.Success
import scala.util.Failure
import akka.actor.Stash
import org.ogmios.core.bean.Status
import org.ogmios.core.action.Read
import org.ogmios.core.action.ReadFrom

/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
class ProviderActor extends Actor with ActorLogging with ActorNames with ConfigCassandraCluster with Stash {
  
  @Override
  def system = context.system
     
  // create a child actor, its name will automatically appended to the ProviderActor path
 
  // TODO need to watch the child???
  // TODO how to provide parameter when we create actor by context.actorOf(Props[CassandraActor], name = cassandraActor)
  val cassandraEndPoint = context.actorOf(Props(new CassandraActor(cluster)), name = cassandraActor)
  
  def receive = {
    case Register(provider: Provider) => {
      
      val name = provider.name
      log.info("register provider " + name)
      
      cassandraEndPoint ! new RegisterFrom(provider, self)
      
      context.become(waitResponse(sender))
    }
    case Read(id: String) => {
      
      log.info("read provider " + id)
      
      cassandraEndPoint ! new ReadFrom(id, self)
      
      context.become(waitResponse(sender))
    }
  }
  
  def waitResponse(providerClient: ActorRef) : Actor.Receive = {
      case resp: Status => {
        providerClient ! resp
        unstashAll()
        context.unbecome
      }
      case resp: Provider => {
        providerClient ! resp
        unstashAll()
        context.unbecome
      }
      case any => stash()
      
  }
}
