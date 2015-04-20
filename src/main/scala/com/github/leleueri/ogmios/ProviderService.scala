package com.github.leleueri.ogmios

import java.time.{ZoneOffset, LocalDateTime, LocalDate}
import java.util.Date
import java.util.concurrent.Executors

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.model.{StatusCode, StatusCodes}
import akka.http.server.Directives._
import akka.http.server.ExceptionHandler
import akka.http.server.PathMatchers.Segment
import akka.http.util.DateTime
import akka.stream.FlowMaterializer
import com.datastax.driver.core.{Session, Row, ResultSet}
import com.datastax.driver.core.exceptions.{NoHostAvailableException, QueryExecutionException, DriverException}
import com.github.leleueri.ogmios.protocol.{EventType, Provider, Protocols}
import com.github.leleueri.scalandra.{CassandraResultSetOperations, ConfigCassandraCluster, CassandraCluster}
import com.typesafe.config.Config
import akka.http.marshallers.sprayjson.SprayJsonSupport._

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future, ExecutionContextExecutor}

import scala.collection.JavaConversions._
import scala.util.Success

/**
 * REST interface to access the Provider resources.
 */
trait ProviderService extends Protocols with ConfigCassandraCluster with CassandraResultSetOperations {

  //implicit val system: ActorSystem

  implicit def executor: ExecutionContextExecutor

  implicit val materializer: FlowMaterializer

  val logger: LoggingAdapter


  // List of columns available int he providers & eventTypes ColumnFamily
  val COL_PROV_ID: String = "id"
  val COL_PROV_NAME: String = "name"
  val COL_PROV_DESC: String = "description"
  val COL_PROV_REG: String = "registration"
  val COL_PROV_EVT: String = "eventTypes"

  val COL_EVT_ID: String = "id"
  val COL_EVT_PROV: String = "provid"
  val COL_EVT_UNIT: String = "unit"
  val COL_EVT_TYPE: String = "type"
  val COL_EVT_REG: String = "registration"


  // -----------------
  // PreparedStatement
  // -----------------

  // We have to set these attributes lazy to avoid NullPointer caused by the ConfigCassandraCluster trait
  def session : Session
  lazy val insertProvStmt = session.prepare("INSERT INTO providers (id, name, description, registration, eventTypes) VALUES (?,?,?,?,?) IF NOT EXISTS")
  lazy val updateProvStmt = session.prepare("UPDATE providers SET description = ?, name = ? WHERE id = ?")
  lazy val readProvStmt = session.prepare("SELECT * FROM providers WHERE id = ?")
  lazy val deleteProvStmt = session.prepare("DELETE FROM providers WHERE id = ?")

  lazy val insertEvtTypeStmt = session.prepare("INSERT INTO eventtypes (id, provid, registration, unit, type) VALUES (?,?,?,?,?) IF NOT EXISTS")
  lazy val readEvtTypeStmt = session.prepare("SELECT * FROM eventtypes WHERE provid = ? and id = ?")
  lazy val deleteEvtTypeStmt = session.prepare("DELETE FROM eventtypes WHERE provid = ? and id = ?")
  lazy val readAllTypesStmt = session.prepare("SELECT * FROM eventtypes WHERE provid = ?")
  lazy val deleteAllTypesStmt = session.prepare("DELETE FROM eventtypes WHERE provid = ?")
  /**
   * This handler complete the route with a status adapted to the received exception
   * Unmanaged exception are computed by the default spray handler
   */
  val providerExceptionHandler = ExceptionHandler {
    case ex : ProviderNotFound => logger.error(ex.getMessage); complete(StatusCodes.NotFound)
    case ex : ProviderInvalid => logger.error(ex.getMessage); complete(StatusCodes.BadRequest)
    case ex : EventTypeInvalid => logger.error(ex.getMessage); complete(StatusCodes.BadRequest)
    case ex : EventTypeNotFound => logger.error(ex.getMessage); complete(StatusCodes.NotFound)
    case ex : QueryExecutionException => logger.error("Cassandra Request can't be processed : " + ex.getMessage, ex); complete(StatusCodes.ServiceUnavailable)
    case ex : NoHostAvailableException => logger.error("Cassandra Request can't be processed : " + ex.getMessage, ex); complete(StatusCodes.ServiceUnavailable)
    case x: Exception => logger.error("Unexpected error: " + x.getMessage, x); complete(StatusCodes.InternalServerError)
  }

  val providerRoutes = {
    handleExceptions(providerExceptionHandler) {
      logRequestResult("akka-http-ogmios") {
        pathPrefix("providers") {
          (put & path(Segment)) {
            provId => entity(as[Provider]) {
              provider => complete {
                Future[StatusCode] {
                  createProvider(provId, provider)
                }
              }
            }
          } ~ (get & path(Segment)) {
            provId => complete {
              readProvider(provId)
            }
          } ~ (post & path(Segment)) {
            provId => entity(as[Provider]) {
              provider => complete {
                Future[StatusCode] {
                  updateProvider(provId, provider)
                }
              }
            }
          } ~ (delete & path(Segment)) {
            provId => complete {
              Future[StatusCode] {
                deleteProvider(provId)
              }
            }
          }
        }
      }
    }
  }

  val allEventTypeRoutes = {
    handleExceptions(providerExceptionHandler) {
      logRequestResult("akka-http-ogmios") {
        path("providers"/Segment/"event-types") {
          provId =>
            get {
              complete {
                readEventsFor(provId)
              }
            } ~ delete {
              complete {
                Future[StatusCode] {
                  deleteEventsFor(provId)
                }
              }
            }
        }
      }
    }
  }

  val eventTypeRoutes = {
    handleExceptions(providerExceptionHandler) {
      logRequestResult("akka-http-ogmios") {
        path("providers"/Segment/"event-types"/Segment) {
          (provId, evtId) => {
            put {
              entity(as[EventType]) {
                event =>
                  complete {
                    createEvent(provId, evtId, event)
                  }
              }
            } ~
              get {
                complete {
                  readEventType(provId, evtId)
                }
              } ~ delete {
              complete {
                Future[StatusCode] {
                  deleteEventType(provId, evtId)
                }
              }
            }
          }
        }
      }
    }
  }

  // --------------------------------
  // Method for the provider resource
  // --------------------------------

  def readProvider(provId: String): Future[Provider] = {
    logger.debug("Read provider '{}'", provId)
    toFuture(session.executeAsync(readProvStmt.bind().setString(COL_PROV_ID, provId)))
      .map(rs => {
        Option(rs.one()).map(row => {
          val id = row.getString(COL_PROV_ID)
          val desc = row.getString(COL_PROV_DESC)
          val name = row.getString(COL_PROV_NAME)
          val date = row.getDate(COL_PROV_REG)
          val evts = row.getSet(COL_PROV_EVT, classOf[String])
          new Provider(id, name, Option(desc), evts.toSet[String], Option(DateTime(date.getTime)))
        }
      ).getOrElse(throw new ProviderNotFound("Provider " + provId + " doesn't exist"))
    })
  }

  def createProvider(provId: String, provider: Provider): StatusCode = {
    if (logger.isDebugEnabled) logger.debug("Creation of provider '{}' : {}", provId, provider)

    if (provider.id != provId) {
      logger.info("Param 'providerId' should be the same as the one present into the bean")
      throw new ProviderInvalid("Param 'providerId' is inconsistent with the one present into the bean")
    }

    val rs = session.execute(insertProvStmt.bind()
      .setString(COL_PROV_ID, provId)
      .setString(COL_PROV_NAME, provider.name)
      .setString(COL_PROV_DESC, provider.desc.orNull)
      .setDate(COL_PROV_REG, new Date(LocalDateTime.parse(provider.creationDate.getOrElse(DateTime.now).toIsoDateTimeString()).toInstant(ZoneOffset.UTC).toEpochMilli)) // ugly...
      .setSet(COL_PROV_EVT, provider.eventTypes))

    if (rs.wasApplied()) StatusCodes.Created else {
      logger.info("provider '{}' already exists", provId)
      StatusCodes.Conflict
    }
  }

  def updateProvider(provId: String, provider: Provider): StatusCode = {
    if (logger.isDebugEnabled) logger.debug("Creation of provider '{}' : {}", provId, provider)

    if (provider.id != provId) {
      logger.info("Param 'providerId' should be the same as the one present into the bean")
      throw new ProviderInvalid("Param 'providerId' is inconsistent with the one present into the bean")
    }

    val rs = session.execute(readProvStmt.bind().setString(COL_PROV_ID, provId))
    if (rs.one() == null) throw new ProviderNotFound("Provider '" + provId + "' doesn't exists")

    // only name & description can be updated
    session.execute(updateProvStmt.bind()
      .setString(COL_PROV_ID, provId)
      .setString(COL_PROV_NAME, provider.name)
      .setString(COL_PROV_DESC, provider.desc.orNull))

    StatusCodes.OK
  }

  def deleteProvider(provId: String): StatusCode = {
    logger.debug("Delete provider '{}'", provId)
    val rs = session.execute(deleteProvStmt.bind().setString(COL_PROV_ID, provId))
    StatusCodes.NoContent
  }


  // -----------------------------------
  // Method for the EventTypes resource
  // -----------------------------------


  def readEventsFor(provId: String): Future[List[EventType]] = {
    logger.debug("Read event types for provider '{}'", provId)

    readProvider(provId).map(
      provider => {
        val iter = session.execute(readAllTypesStmt.bind().setString(COL_EVT_PROV, provId)).iterator()
        createEventList(iter, List())
      }
    )
  }

  @tailrec
  final def createEventList(iter : java.util.Iterator[Row], acc : List[EventType]) : List[EventType] = {
    if (!iter.hasNext) acc
    else {
      val row = iter.next()
      val evt = new EventType(row.getString(COL_EVT_ID), Option(row.getString(COL_EVT_UNIT)), row.getString(COL_EVT_TYPE), row.getString(COL_EVT_PROV),  Option(DateTime(row.getDate(COL_EVT_REG).getTime)))
      createEventList(iter, evt :: acc)
    }
  }

  def deleteEventsFor(provId: String): StatusCode = {
    logger.debug("Delete event types for provider '{}'", provId)
    val rs = session.execute(deleteAllTypesStmt.bind().setString(COL_EVT_PROV, provId))
    StatusCodes.NoContent
  }

  def deleteEventType(provId: String, evt: String): StatusCode = {
    logger.debug("Delete event '{}' for provider '{}'", evt, provId)
    val rs = session.execute(deleteEvtTypeStmt.bind().setString(COL_EVT_PROV, provId).setString(COL_EVT_ID, evt))
    StatusCodes.NoContent
  }

  def readEventType(provId: String, evt: String): Future[EventType] = {
    logger.debug("Read event '{}' for provider '{}'", evt, provId)
    toFuture(session.executeAsync(readEvtTypeStmt.bind().setString(COL_EVT_PROV, provId).setString(COL_EVT_ID, evt)))
      .map(rs => {
      Option(rs.one()).map(row => {
        new EventType(row.getString(COL_EVT_ID), Option(row.getString(COL_EVT_UNIT)), row.getString(COL_EVT_TYPE), row.getString(COL_EVT_PROV),  Option(DateTime(row.getDate(COL_EVT_REG).getTime)))
      }
      ).getOrElse(throw new EventTypeNotFound("EventType " + evt + " doesn't exist for the provider " + provId))
    })
  }

  def createEvent(provId: String, evtId: String, event: EventType): Future[StatusCode] = {
    if (logger.isDebugEnabled) logger.debug("Creation of event '{}' for provider '{}'", evtId, provId)

    if (event.provider != provId) {
      logger.info("Param 'providerId' should be the same as the one present into the bean")
      throw new EventTypeInvalid("Param 'providerId' is inconsistent with the one present into the bean")
    }

    if (event.id != evtId) {
      logger.info("Param 'eventId' should be the same as the one present into the bean")
      throw new EventTypeInvalid("Param 'eventId' is inconsistent with the one present into the bean")
    }

    // test if the provider has been declared before inserting the new type of event.
    readProvider(provId).map(
      p => {
        val rs = session.execute(insertEvtTypeStmt.bind()
          .setString(COL_EVT_ID, evtId)
          .setString(COL_EVT_PROV, provId)
          .setDate(COL_EVT_REG, new Date(LocalDateTime.parse(event.creationDate.getOrElse(DateTime.now).toIsoDateTimeString()).toInstant(ZoneOffset.UTC).toEpochMilli)) // ugly...
          .setString(COL_EVT_UNIT, event.unit.orNull).setString(COL_EVT_TYPE, event.valueType))

        if (rs.wasApplied()) StatusCodes.Created
        else {
          logger.info("EventType '{}' already exists", provId)
          StatusCodes.Conflict
        }
      })
  }
}