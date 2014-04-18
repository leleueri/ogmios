package org.ogmios.core.actor
import akka.actor.Actor
import com.datastax.driver.core.Cluster
import org.ogmios.core.bean.Provider
import org.ogmios.core.action.RegisterFrom
import java.util.concurrent.TimeUnit
import scala.concurrent.Future
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.ResultSetFuture
import akka.actor.ActorRef
import scala.util.Success
import scala.util.Failure

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

class CassandraActor (cluster: Cluster) extends Actor with CassandraResultSetOperations {
        
  implicit val exec = context.dispatcher
 
  val session = cluster.connect(Keyspaces.ogmios)
  
  // TODO addition of REF
  val preparedStatement = session.prepare("INSERT INTO providers(id, name, registration) VALUES (?, ?, ?);")

  def saveProvider(provider: Provider, providerActor: ActorRef): Unit = {
    // TODO ADD REF if non empty
    val f = toFuture(session.executeAsync(preparedStatement.bind(provider.id, provider.name, provider.creation)))
    
    f.onComplete {
      case Success(_) => providerActor ! "OK"
      case Failure(_) => providerActor ! "KO"
    }
  }
  
  def receive: Receive = {
    case RegisterFrom(provider: Provider, theSender) => saveProvider(provider, theSender)
  }
}