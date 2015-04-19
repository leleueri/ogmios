package com.github.leleueri.ogmios

import java.time.{ZoneOffset, LocalDateTime}
import java.util.Date

import akka.actor.ActorSystem
import akka.event.{LoggingAdapter, Logging}
import akka.http.Http
import akka.http.client.RequestBuilding
import akka.http.marshallers.sprayjson.SprayJsonSupport._
import akka.http.marshalling.ToResponseMarshallable
import akka.http.model.{StatusCode, StatusCodes, HttpResponse, HttpRequest}
import akka.http.model.StatusCodes._
import akka.http.server.Directives._
import akka.http.server.ExceptionHandler
import akka.http.util.DateTime
import akka.stream.{ActorFlowMaterializer, FlowMaterializer}
import akka.stream.scaladsl.{Sink, Source}
import com.datastax.driver.core.{Session, Row}
import com.datastax.driver.core.exceptions.{NoHostAvailableException, QueryExecutionException}
import com.github.leleueri.ogmios.protocol.{EventType, Protocols, Provider}
import com.github.leleueri.scalandra.{CassandraResultSetOperations, ConfigCassandraCluster}
import com.typesafe.config.Config
import sun.text.resources.vi.CollationData_vi
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContextExecutor, Future}



trait EventTypeService extends Protocols with ConfigCassandraCluster with CassandraResultSetOperations {

  implicit val system: ActorSystem

  implicit def executor: ExecutionContextExecutor

  implicit val materializer: FlowMaterializer

  val logger: LoggingAdapter


  // List of columns available int he providers ColumnFamily
  val COL_EVT_ID: String = "id"
  val COL_PROV: String = "provid"
  val COL_UNIT: String = "unit"
  val COL_TYPE: String = "type"
  val COL_EVT_REG: String = "registration"

  // -----------------
  // PreparedStatement
  // -----------------

  // We have to set these attributes lazy to avoid NullPointer caused by the ConfigCassandraCluster trait
  def session : Session
  lazy val insertEvtTypeStmt = session.prepare("INSERT INTO eventtypes (id, provid, registration, unit, type) VALUES (?,?,?,?,?) IF NOT EXISTS")
  lazy val readEvtTypeStmt = session.prepare("SELECT * FROM eventtypes WHERE provid = ? and id = ?")
  lazy val deleteEvtTypeStmt = session.prepare("DELETE FROM eventtypes WHERE provid = ? and id = ?")
  lazy val readAllTypesStmt = session.prepare("SELECT * FROM eventtypes WHERE provid = ?")
  lazy val deleteAllTypesStmt = session.prepare("DELETE FROM eventtypes WHERE provid = ?")

  /**
   * This handler complete the route with a status adapted to the received exception
   * Unmanaged exception are computed by the default spray handler
   */
  val evtTypeExceptionHandler = ExceptionHandler {
    case ex : ProviderNotFound => logger.error(ex.getMessage); complete(StatusCodes.NotFound)
    case ex : EventTypeInvalid => logger.error(ex.getMessage); complete(StatusCodes.BadRequest)
    case ex : EventTypeNotFound => logger.error(ex.getMessage); complete(StatusCodes.NotFound)
    case ex : QueryExecutionException => logger.error("Cassandra Request can't be processed : " + ex.getMessage, ex); complete(StatusCodes.ServiceUnavailable)
    case ex : NoHostAvailableException => logger.error("Cassandra Request can't be processed : " + ex.getMessage, ex); complete(StatusCodes.ServiceUnavailable)
    case x: Exception => logger.error("Unexpected error: " + x.getMessage, x); complete(StatusCodes.InternalServerError)
  }

  val allEventTypeRoutes = {
    handleExceptions(evtTypeExceptionHandler) {
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
    handleExceptions(evtTypeExceptionHandler) {
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


  def readEventsFor(provId: String): Future[List[EventType]] = {
    logger.debug("Read event types for provider '{}'", provId)
    toFuture(session.executeAsync(readAllTypesStmt.bind().setString(COL_PROV, provId)))
      .map(rs => {
          val iter = rs.iterator()
          createEventList(iter, List())
        }
      )
  }

  @tailrec
  final def createEventList(iter : java.util.Iterator[Row], acc : List[EventType]) : List[EventType] = {
    if (!iter.hasNext) acc
    else {
      val row = iter.next()
      val evt = new EventType(row.getString(COL_EVT_ID), Option(row.getString(COL_UNIT)), row.getString(COL_TYPE), row.getString(COL_PROV),  Option(DateTime(row.getDate(COL_EVT_REG).getTime)))
      createEventList(iter, evt :: acc)
    }
  }

  def deleteEventsFor(provId: String): StatusCode = {
    logger.debug("Delete event types for provider '{}'", provId)
    val rs = session.execute(deleteAllTypesStmt.bind().setString(COL_PROV, provId))
    StatusCodes.NoContent
  }

  def deleteEventType(provId: String, evt: String): StatusCode = {
    logger.debug("Delete event '{}' for provider '{}'", evt, provId)
    val rs = session.execute(deleteEvtTypeStmt.bind().setString(COL_PROV, provId).setString(COL_EVT_ID, evt))
    StatusCodes.NoContent
  }

  def readEventType(provId: String, evt: String): Future[EventType] = {
    logger.debug("Read event '{}' for provider '{}'", evt, provId)
    toFuture(session.executeAsync(readEvtTypeStmt.bind().setString(COL_PROV, provId).setString(COL_EVT_ID, evt)))
      .map(rs => {
        Option(rs.one()).map(row => {
          new EventType(row.getString(COL_EVT_ID), Option(row.getString(COL_UNIT)), row.getString(COL_TYPE), row.getString(COL_PROV),  Option(DateTime(row.getDate(COL_EVT_REG).getTime)))
        }
      ).getOrElse(throw new EventTypeNotFound("EventType " + evt + " doesn't exist for the provider " + provId))
    })
  }

  def createEvent(provId: String, evtId: String, event: EventType): StatusCode = {
    if (logger.isDebugEnabled) logger.debug("Creation of event '{}' for provider '{}'", evtId, provId)

    if (event.provider != provId) {
      logger.info("Param 'providerId' should be the same as the one present into the bean")
      throw new EventTypeInvalid("Param 'providerId' is inconsistent with the one present into the bean")
    }

    if (event.id != evtId) {
      logger.info("Param 'eventId' should be the same as the one present into the bean")
      throw new EventTypeInvalid("Param 'eventId' is inconsistent with the one present into the bean")
    }

    // TODO check if the provider exist

    val rs = session.execute(insertEvtTypeStmt.bind()
      .setString(COL_EVT_ID, evtId)
      .setString(COL_PROV, provId)
      .setDate(COL_EVT_REG, new Date(LocalDateTime.parse(event.creationDate.getOrElse(DateTime.now).toIsoDateTimeString()).toInstant(ZoneOffset.UTC).toEpochMilli)) // ugly...
      .setString(COL_UNIT, event.unit.orNull).setString(COL_TYPE, event.valueType))

    if (rs.wasApplied()) StatusCodes.Created else {
      logger.info("provider '{}' already exists", provId)
      StatusCodes.Conflict
    }
  }
}

