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
import com.datastax.driver.core.{Row, ResultSet}
import com.datastax.driver.core.exceptions.{NoHostAvailableException, QueryExecutionException, DriverException}
import com.github.leleueri.ogmios.protocol.Provider
import com.github.leleueri.ogmios.protocol.Protocols
import com.github.leleueri.scalandra.{CassandraResultSetOperations, ConfigCassandraCluster, CassandraCluster}
import com.typesafe.config.Config
import akka.http.marshallers.sprayjson.SprayJsonSupport._

import scala.concurrent.{ExecutionContext, Future, ExecutionContextExecutor}

import scala.collection.JavaConversions._

/**
 * REST interface to access the Provider resources.
 */
trait ProviderService extends Protocols with ConfigCassandraCluster with CassandraResultSetOperations {

  implicit val system: ActorSystem

  implicit def executor: ExecutionContextExecutor

  implicit val materializer: FlowMaterializer

  val logger: LoggingAdapter

  val keyspace = "ogmios"

  // List of columns available int he providers ColumnFamily
  val COL_ID: String = "id"
  val COL_NAME: String = "name"
  val COL_DESC: String = "description"
  val COL_REG: String = "registration"
  val COL_EVT: String = "eventTypes"

  // -----------------
  // PreparedStatement
  // -----------------

  // We have to set these attributes lazy to avoid NullPointer caused by the ConfigCassandraCluster trait
  lazy val session = cluster.connect(keyspace)
  lazy val insertStmt = session.prepare("INSERT INTO providers (id, name, description, registration, eventTypes) VALUES (?,?,?,?,?) IF NOT EXISTS")
  lazy val updateStmt = session.prepare("UPDATE providers SET description = ?, name = ? WHERE id = ?")
  lazy val readStmt = session.prepare("SELECT * FROM providers WHERE id = ?")
  lazy val deleteStmt = session.prepare("DELETE FROM providers WHERE id = ?")

  /**
   * This handler complete the route with a status adapted to the received exception
   * Unmanaged exception are computed by the default spray handler
   */
  val ogmiosExceptionHandler = ExceptionHandler {
    case ex : ProviderNotFound => logger.error(ex.getMessage); complete(StatusCodes.NotFound)
    case ex : ProviderInvalid => logger.error(ex.getMessage); complete(StatusCodes.BadRequest)
    case ex : QueryExecutionException => logger.error("Cassandra Request can't be processed : " + ex.getMessage, ex); complete(StatusCodes.ServiceUnavailable)
    case ex : NoHostAvailableException => logger.error("Cassandra Request can't be processed : " + ex.getMessage, ex); complete(StatusCodes.ServiceUnavailable)
    case x: Exception => logger.error("Unexpected error: " + x.getMessage, x); complete(StatusCodes.InternalServerError)
  }

  val providerRoutes = {
    handleExceptions(ogmiosExceptionHandler) {
      logRequestResult("akka-http-microservice") {
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

  def readProvider(provId: String): Future[Provider] = {
    logger.debug("Read provider '{}'", provId)
    toFuture(session.executeAsync(readStmt.bind().setString(COL_ID, provId)))
      .map(rs => {
        Option(rs.one()).map(row => {
          val id = row.getString(COL_ID)
          val desc = row.getString(COL_DESC)
          val name = row.getString(COL_NAME)
          val date = row.getDate(COL_REG)
          val evts = row.getSet(COL_EVT, classOf[String])
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

    val rs = session.execute(insertStmt.bind()
      .setString(COL_ID, provId)
      .setString(COL_NAME, provider.name)
      .setString(COL_DESC, provider.desc.orNull)
      .setDate(COL_REG, new Date(LocalDateTime.parse(provider.creationDate.getOrElse(DateTime.now).toIsoDateTimeString()).toInstant(ZoneOffset.UTC).toEpochMilli)) // ugly...
      .setSet(COL_EVT, provider.eventTypes))

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

    val rs = session.execute(readStmt.bind().setString(COL_ID, provId))
    if (rs.one() == null) throw new ProviderNotFound("Provider '" + provId + "' doesn't exists")

    // only name & description can be updated
    session.execute(updateStmt.bind()
      .setString(COL_ID, provId)
      .setString(COL_NAME, provider.name)
      .setString(COL_DESC, provider.desc.orNull))

    StatusCodes.OK
  }

  def deleteProvider(provId: String): StatusCode = {
    logger.debug("Delete provider '{}'", provId)
    val rs = session.execute(deleteStmt.bind().setString(COL_ID, provId))
    StatusCodes.NoContent
  }
}