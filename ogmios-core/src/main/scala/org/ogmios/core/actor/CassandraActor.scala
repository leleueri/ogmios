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
import akka.actor.ActorLogging
import org.ogmios.core.bean.Status
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

class CassandraActor (cluster: Cluster) extends Actor  with ActorLogging with CassandraResultSetOperations {
        
  implicit val exec = context.dispatcher
 
  val session = cluster.connect(Keyspaces.ogmios)
  
  // TODO addition of REF
  val insertProviderStmt = session.prepare("INSERT INTO providers(id, name, registration) VALUES (?, ?, ?);")
  val selectProviderStmt = session.prepare("SELECT * FROM providers WHERE id = ?;")

  def saveProvider(provider: Provider, providerActor: ActorRef): Unit = {
    // TODO ADD REF if non empty
    val f = toFuture(session.executeAsync(insertProviderStmt.bind(provider.id, provider.name, provider.creation)))
    
    f.onComplete {
      case Success(_) => providerActor ! new Status("OK", "")
      case Failure(e) => providerActor ! new Status("KO", e.getMessage())
    }
  }
  
  def readProvider(id: String, providerActor: ActorRef): Unit = {
    val f = toFuture(session.executeAsync(selectProviderStmt.bind(id)))
   
    f.onComplete {
      case Success(rs) => {
            val extractedLocalValue = rs.iterator() 
            if ( extractedLocalValue.hasNext()) {
                val row = extractedLocalValue.next()
                val readProvider = new Provider(row.getString("id"),
                row.getString("name"),
                row.getDate("registration"),
                None) // TODO ADD REF if non empty
                
                providerActor ! readProvider
            } else  {
                providerActor ! new Status("NOT_FOUND", "Unknown provider id '" + id +"'")
            }
        }
      case Failure(e) => providerActor ! new Status("KO", e.getMessage())
    }
  }
  
  def receive: Receive = {
    case RegisterFrom(provider: Provider, theSender) => saveProvider(provider, theSender)
    case ReadFrom(id: String, theSender) => readProvider(id, theSender)
  
  }
}