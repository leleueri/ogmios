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
import com.datastax.driver.core.Row
import scala.util.Try
import org.ogmios.core.bean.OpCompleted
import scala.util.Success
import scala.util.Failure
import org.ogmios.core.action.Update
import java.util.Map

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
  
  val insertProviderStmt = session.prepare("INSERT INTO providers(id, name, registration, ref) VALUES (?, ?, ?, ?) IF NOT EXISTS;")
  val updateProviderStmt = session.prepare("UPDATE providers SET ref = ? WHERE id = ?;")
  val selectProviderStmt = session.prepare("SELECT * FROM providers WHERE id = ?;")

  
  /**
   * This method allow to create a row controlling if the provider id is available.
   * If the row exists, a OpFailed is returned with the Conflict state.
   * If the row is missing, a OpCompleted is returned with the Ok state. (the provider is now present in the datastore)
   * Otherwise a OpFailed with the Ko state will be provided if an exception occurs
   */
  def saveProvider(provider: Provider): Future[Status] = {
    val saveFuture = provider.ref match {
      case Some(map) => toFuture(session.executeAsync(insertProviderStmt.bind(provider.id, provider.name, new Date(provider.creation), mapAsJavaMap(map))))
      case None => toFuture(session.executeAsync(insertProviderStmt.bind(provider.id, provider.name, new Date(provider.creation), null)))
    }
    saveFuture.map { 
      (rs : ResultSet) => 
        val row = rs.one();
        if (row.getBool("[applied]")) 
          new OpCompleted(Status.StateOk, "") 
        else 
          new OpFailed(Status.StateConflict, "provider '"+provider.id+"' already exists")
    } recover {
      case e => new OpFailed(Status.StateKo, e.getMessage())
    }
  }
  
  /**
   * This method allow to update the ref field of the given provider.
   * If the row doesn't exist, a OpFailed is returned with the NotFound state.
   * If the row exists, a OpCompleted is returned with the Ok state.
   * Otherwise a OpFailed with the Ko state will be provided if an exception occurs
   */
  def updateProvider(provider: Provider): Future[Status] = {
    val promise = Promise[Status]
    // TODO use map/flatMap to compute try results
    Future {
        Try(session.execute(selectProviderStmt.bind(provider.id)))
        match {
          case Success(rs) => {
              val extractedLocalValue = rs.iterator() 
              if ( extractedLocalValue.hasNext()) {
                  log.debug(s"provider ${provider.id} exists, update can be performed")
                  Try(
                      provider.ref match {
                          case Some(map) => session.execute(updateProviderStmt.bind(mapAsJavaMap(map), provider.id))
                          case None => session.execute(updateProviderStmt.bind(null, provider.id))
                      }
                  ) match {
                    case Success(res) => promise success new OpCompleted(Status.StateOk, "") 
                    case Failure(exc) => log.error(s"Unable to update Provider ${provider.id}", exc)
                                         promise success new OpCompleted(Status.StateKo, exc.getMessage()) 
                  }
               } else  {
                  log.debug(s"provider with id ${provider.id} is unknown");
                  promise success new OpFailed(Status.StateNotFound, "Unknown provider id '" + provider.id +"'")
               }
          }
          case Failure(exc) => log.error(s"Unable to update Provider ${provider.id}", exc)
                               promise success new OpCompleted(Status.StateKo, exc.getMessage()) 
        }
    } 
    promise.future
  }
  
  /**
   * This method allow to read a row with the id value as primary key.
   * If the row exists, a OpResult is returned with an instance of Provider.
   * If the row is missing, a OpFailed is returned with the NotFound state.
   * Otherwise a OpFailed with the Ko state will be provided if an exception occurs
   */
  def readProvider(id: String): Future[Status] = {
    val readFuture = toFuture(session.executeAsync(selectProviderStmt.bind(id)))
    readFuture.map {
      (rs : ResultSet) => { 
            val extractedLocalValue = rs.iterator() 
            if ( extractedLocalValue.hasNext()) {
              
                val row = extractedLocalValue.next()
                
                val map = if (row.isNull("ref")) None 
                          else Some(row.getMap("ref", classOf[String], classOf[String]).toMap)
                
                val readProvider = new Provider(row.getString("id"),
                                                row.getString("name"),
                                                row.getDate("registration").getTime(),
                                                map)
                
                new OpResult(Status.StateOk, "", readProvider)
            } else  {
              log.debug(s"provider with id ${id} is unknown");
              new OpFailed(Status.StateNotFound, "Unknown provider id '" + id +"'")
            }
        }
    } recover {
      case e => log.error(s"Unable to read provider with id ${id}", e); new OpFailed(Status.StateKo, e.getMessage())
    }
  }
  
  def receive: Receive = {
    case Register(provider: Provider) => saveProvider(provider) pipeTo sender
    case Update(provider: Provider) => updateProvider(provider) pipeTo sender
    case Read(id: String) => readProvider(id) pipeTo sender
  }
}