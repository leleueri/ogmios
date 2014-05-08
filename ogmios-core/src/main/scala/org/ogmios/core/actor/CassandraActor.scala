package org.ogmios.core.actor

import java.util.Date

import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConversions.mapAsScalaMap
import scala.concurrent.Future
import scala.concurrent.Promise

import org.ogmios.core.action.Read
import org.ogmios.core.action.Register
import org.ogmios.core.bean.OpCompleted
import org.ogmios.core.bean.OpFailed
import org.ogmios.core.bean.OpResult
import org.ogmios.core.bean.Provider
import org.ogmios.core.bean.Status
import org.ogmios.core.config.ConfigCassandraCluster

import com.datastax.driver.core.ResultSet

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.pattern.pipe

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

class CassandraActor extends Actor with ActorLogging with ConfigCassandraCluster with CassandraResultSetOperations {
        
  implicit val exec = context.dispatcher
  
  override def system = context.system
  
  val session = cluster.connect(Keyspaces.ogmios)
  
  val insertProviderStmt = session.prepare("INSERT INTO providers(id, name, registration) VALUES (?, ?, ?);")
  val insertFullProviderStmt = session.prepare("INSERT INTO providers(id, name, registration, ref) VALUES (?, ?, ?, ?);")
  val selectProviderStmt = session.prepare("SELECT * FROM providers WHERE id = ?;")

  def saveProvider(provider: Provider): Future[Status] = {
    
    val f = provider.ref match {
      case Some(map) => toFuture(session.executeAsync(insertFullProviderStmt.bind(provider.id, provider.name, new Date(provider.creation), mapAsJavaMap(map))))
      case None => toFuture(session.executeAsync(insertProviderStmt.bind(provider.id, provider.name, new Date(provider.creation))))
    }
    
    f.map { 
      (x : ResultSet) => new OpCompleted(Status.StateOk, "")
    } recover {
      case e => new OpFailed(Status.StateKo, e.getMessage())
    }
  }
  
  def readProvider(id: String): Future[Status] = {
    val f = toFuture(session.executeAsync(selectProviderStmt.bind(id)))

    f.flatMap {
      (rs : ResultSet) => { 
            val extractedLocalValue = rs.iterator() 
            if ( extractedLocalValue.hasNext()) {
              
                val row = extractedLocalValue.next()
                
                val map = if (row.isNull("ref")) None 
                          else {
                            val jmap = row.getMap("ref", classOf[String], classOf[String])
                            Some(jmap.toMap)
                          } 
                
                val readProvider = new Provider(row.getString("id"),
                                                row.getString("name"),
                                                row.getDate("registration").getTime(),
                                                map)
                
                Future {new OpResult(Status.StateOk, "", readProvider)}
            } else  {
                Future {new OpFailed(Status.StateNotFound, "Unknown provider id '" + id +"'")}
            }
        }
    } recoverWith {
      case e:Exception => Future {new OpFailed(Status.StateKo, e.getMessage())}
    }
  }
  
  def receive: Receive = {
    case Register(provider: Provider) => saveProvider(provider) pipeTo sender
    case Read(id: String) => readProvider(id) pipeTo sender
  
  }
}