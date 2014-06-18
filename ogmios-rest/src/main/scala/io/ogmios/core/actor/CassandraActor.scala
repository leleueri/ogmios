package io.ogmios.core.actor

import java.util.Date
import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.concurrent.Promise
import io.ogmios.core.action.Read
import io.ogmios.core.action.Register
import io.ogmios.core.bean.OpCompleted
import io.ogmios.core.bean.OpFailed
import io.ogmios.core.bean.OpResult
import io.ogmios.core.bean.Provider
import io.ogmios.core.bean.OgmiosStatus
import io.ogmios.core.config.ConfigCassandraCluster
import com.datastax.driver.core.ResultSet
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.pattern.pipe
import com.datastax.driver.core.Row
import scala.util.Try
import io.ogmios.core.bean.OpCompleted
import scala.util.Success
import scala.util.Failure
import io.ogmios.core.action.Update
import io.ogmios.core.bean.Message
import io.ogmios.core.bean.Metric
import io.ogmios.core.bean.Event
import scala.util.Success
import scala.util.Failure
import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.BoundStatement
import io.ogmios.core.bean.OpResult
import io.ogmios.core.bean.Event
import io.ogmios.core.action.ReadEventsTimeline
import io.ogmios.core.action.ReadMetricsTimeline
import io.ogmios.exception.ConflictException
import spray.http.StatusCodes
import io.ogmios.exception.NotFoundException
import io.ogmios.exception.InternalErrorException
import io.ogmios.exception.OgmiosException
import com.datastax.driver.core.ConsistencyLevel
import io.ogmios.core.config.OgmiosConfig
import io.ogmios.core.action.DeleteProvider
import com.datastax.driver.core.ResultSetFuture

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

class CassandraActor extends Actor with ActorLogging 
with ConfigCassandraCluster with CassandraResultSetOperations
with OgmiosConfig {
        
  implicit val exec = context.dispatcher
  
  override def system = context.system
  
  val session = cluster.connect(Keyspaces.ogmios)
  
  val insertProviderStmt = session.prepare("INSERT INTO providers(id, name, registration, ref) VALUES (?, ?, ?, ?) IF NOT EXISTS;").setConsistencyLevel(ConsistencyLevel.QUORUM)
  val updateProviderStmt = session.prepare("UPDATE providers SET ref = ? WHERE id = ?;").setConsistencyLevel(ConsistencyLevel.QUORUM)
  val selectProviderStmt = session.prepare("SELECT * FROM providers WHERE id = ?;").setConsistencyLevel(ConsistencyLevel.QUORUM)
  val deleteProviderStmt = session.prepare("DELETE FROM providers WHERE id = ?;").setConsistencyLevel(ConsistencyLevel.QUORUM)
 
  val insertMetricStmt = session.prepare("INSERT INTO metrics(providerid, metricname, registration, generation, value) VALUES (?, ?, ?, ?, ?) USING TTL ?").setConsistencyLevel(ConsistencyLevel.QUORUM)
  val insertEventStmt = session.prepare("INSERT INTO events(providerid, eventname, registration, generation, properties) VALUES (?, ?, ?, ?, ?) USING TTL ?").setConsistencyLevel(ConsistencyLevel.QUORUM)
  // No limit on these query because Cassandra manage auto paging
  val eventsRangeStmt = session.prepare("SELECT * FROM events WHERE providerid = ? AND eventname = ? AND generation >= ? AND generation <= ?;").setConsistencyLevel(ConsistencyLevel.QUORUM)
  val metricsRangeStmt = session.prepare("SELECT * FROM metrics WHERE providerid = ? AND metricname = ? AND generation >= ? AND generation <= ?;").setConsistencyLevel(ConsistencyLevel.QUORUM)

  /**
   * Save the Message if the Provider exists 
   */
  def saveMessage(message: Message): Future[OgmiosStatus] = {
    def checkProviderAndExec(statement: BoundStatement) : OgmiosStatus = {
      val resultSet = session.execute(selectProviderStmt.bind(message.provider))
      if (resultSet.iterator().hasNext()) {
          session.execute(statement)
          new OpCompleted(OgmiosStatus.StateOk) 
      } else {
          throw new NotFoundException(s"Provider ${message.provider} is unknown")
      }
    }
    
    Future {
      message match {
        // TODO make TTL configurable during the Event/Metric declaration ??
        case m: Metric => checkProviderAndExec(insertMetricStmt.bind(m.provider, m.name, new Date(), new Date(m.emission),  m.value:java.lang.Double, eventsTtl:java.lang.Integer))
        case e: Event => checkProviderAndExec(insertEventStmt.bind(e.provider, e.name, new Date(), new Date(e.emission), mapAsJavaMap(e.properties), metricsTtl:java.lang.Integer))
      }
    }
  }
  
  def readMetrics(provider: String, name: String, begin: Long, end: Option[Long]): Future[OgmiosStatus] = {
    val endOrNow = end.getOrElse(System.currentTimeMillis())    
    // reorder range parameters
    val startDate = new Date(begin.min(endOrNow))
    val endDate = new Date(begin.max(endOrNow))
    
    Future {
      val resultSet = session.execute(selectProviderStmt.bind(provider))
      if (resultSet.iterator().hasNext()) {
        val rs = session.execute(metricsRangeStmt.bind(provider, name, startDate, endDate))
        val metrics = for {
          r <- rs.all
        } yield new Metric(r.getString("providerid"), r.getDate("generation").getTime, r.getString("metricname"), r.getDouble("value")) 
        new OpResult(OgmiosStatus.StateOk, metrics.toList) 
      } else {
        throw new NotFoundException(s"Provider ${provider} is unknown")  
      }
    }
  }
  
  def readEvents(provider: String, name: String, begin: Long, end: Option[Long]): Future[OgmiosStatus] = {
    val endOrNow = end.getOrElse(System.currentTimeMillis())    
    // reorder range parameters
    val startDate = new Date(begin.min(endOrNow))
    val endDate = new Date(begin.max(endOrNow))
    
    Future {
      val resultSet = session.execute(selectProviderStmt.bind(provider))
      if (resultSet.iterator().hasNext()) {
        val rs = session.execute(eventsRangeStmt.bind(provider, name, startDate, endDate))
        val events = for {
          r <- rs.all
        } yield new Event(r.getString("providerid"), r.getDate("generation").getTime, r.getString("eventname"), r.getMap("properties", classOf[String], classOf[String]).toMap) 
        new OpResult(OgmiosStatus.StateOk, events.toList) 
      } else {
        throw new NotFoundException(s"Provider ${provider} is unknown")  
      }
    }
  }
  
  /**
   * This method allow to create a row controlling if the provider id is available.
   * If the row exists, a Conflict Exception will be thrown by the future.
   * If the row is missing, a OpCompleted is returned with the Ok state. (the provider is now present in the datastore)
   * 
   */
  def saveProvider(provider: Provider): Future[OgmiosStatus] = {
    val saveFuture = toFuture(session.executeAsync(
            insertProviderStmt.bind(provider.id, provider.name, new Date(provider.creation), mapAsJavaMap(provider.ref.getOrElse(Map()))))) 
    
    saveFuture.map { 
      (rs : ResultSet) => 
        val row = rs.one();
        if (row.getBool("[applied]")) 
          new OpCompleted(OgmiosStatus.StateOk) 
        else 
          throw new ConflictException("provider '"+provider.id+"' already exists")
    } 
  }
  
  /**
   * This method allow to update the ref field of the given provider.
   * If the row exists, a OpCompleted is returned with the Ok state.
   * Otherwise an OgmiosException will be thrown
   */
  def updateProvider(provider: Provider): Future[OgmiosStatus] = {
    Future {
        val rs = session.execute(selectProviderStmt.bind(provider.id))
        val extractedLocalValue = rs.iterator() 
        if ( extractedLocalValue.hasNext()) {
          log.debug(s"provider ${provider.id} exists, update can be performed")
          session.execute(updateProviderStmt.bind(mapAsJavaMap(provider.ref.getOrElse(Map())), provider.id))
          new OpCompleted(OgmiosStatus.StateOk)
        } else  {
          log.debug(s"provider with id ${provider.id} is unknown");
          throw new NotFoundException(s"Unknown provider id '${provider.id}'")
        }
    } 
  }
  
  def deleteProvider(provider: String): Future[OgmiosStatus] = {
    Future {
        val rs = session.execute(selectProviderStmt.bind(provider))
        val extractedLocalValue = rs.iterator() 
        if ( extractedLocalValue.hasNext()) {
          log.debug(s"provider ${provider} exists, delete can be performed")
          
          // metrics and events are saved with a ttl so cassandra will removed them automatically
          
          session.execute(deleteProviderStmt.bind(provider))
          
          new OpCompleted(OgmiosStatus.StateOk)
        } else  {
          log.debug(s"provider with id ${provider} is unknown, return NO_CONTENT status");
          new OpCompleted(OgmiosStatus.StateOk)
        }
    } 
  }
  
  /**
   * This method allow to read a row with the id value as primary key.
   * If the row exists, a OpResult is returned with an instance of Provider.
   */
  def readProvider(id: String): Future[OgmiosStatus] = {
    val readFuture = toFuture(session.executeAsync(selectProviderStmt.bind(id)))
    readFuture.map {
      (rs : ResultSet) => { 
            val extractedLocalValue = rs.iterator() 
            if ( extractedLocalValue.hasNext()) {
                val row = extractedLocalValue.next()
                val map = if (row.isNull("ref")) None else Some(row.getMap("ref", classOf[String], classOf[String]).toMap)
                val readProvider = new Provider(row.getString("id"),
                                                row.getString("name"),
                                                row.getDate("registration").getTime(),
                                                map)
                
                new OpResult(OgmiosStatus.StateOk, readProvider)
            } else  {
              log.debug(s"provider with id ${id} is unknown");
              throw new NotFoundException(s"Unknown provider id '{id}'")
            }
        }
    }
  }
  
  def receive: Receive = {
    case Register(msg: Message) => saveMessage(msg) pipeTo sender
    case Register(provider: Provider) => saveProvider(provider) pipeTo sender
    case Update(provider: Provider) => updateProvider(provider) pipeTo sender
    case Read(id: String) => readProvider(id) pipeTo sender
    case DeleteProvider(id: String) => deleteProvider(id) pipeTo sender
    case ReadEventsTimeline(provider, name, begin, end) => readEvents(provider, name, begin, end) pipeTo sender
    case ReadMetricsTimeline(provider, name, begin, end) => readMetrics(provider, name, begin, end) pipeTo sender
  }
}