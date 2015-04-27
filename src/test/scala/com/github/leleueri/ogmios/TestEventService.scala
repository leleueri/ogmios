package com.github.leleueri.ogmios

import java.util.UUID

import akka.event.NoLogging
import akka.http.model.ContentTypes._
import akka.http.model.StatusCodes._
import akka.http.testkit.ScalatestRouteTest
import com.github.leleueri.ogmios.protocol.{Event, EventType, Provider}
import org.scalatest._

import scala.collection.mutable

class TestEventService extends FlatSpec with Matchers with ScalatestRouteTest with ProviderService with BeforeAndAfterEach {

  override def testConfigSource = "akka.loglevel = DEBUG"
  override def config = testConfig
  override val logger = NoLogging

  val keyspace = "ogmios"
  lazy val session = cluster.connect(keyspace)

  val providerOneId = "providerid-"+UUID.randomUUID().toString
  val providerOneBean = new Provider(providerOneId, "name-"+providerOneId, None, Set.empty, None)

  val eventTypeId: String = "event-type-" + UUID.randomUUID().toString
  val eventTypeBean = new EventType(eventTypeId, None, "int", providerOneId, None)

  override def afterEach() {
    // after each test case, remove the provider from cassandra
    session.execute(deleteProvStmt.bind().setString(COL_PROV_ID, providerOneId))
    // after each test case, remove all types of event fo the provider from cassandra
    session.execute(deleteAllTypesStmt.bind().setString(COL_EVT_TYPE_PROV, providerOneId))
    session.execute(deleteAllEventsStmt.bind().setString(COL_EVT_PROV, providerOneId).setString(COL_EVT_ETID, eventTypeId))
  }


  "Service" should "respond Create and Read Events for a given provider & event type" in {
    Put(s"/providers/$providerOneId", providerOneBean) ~> providerRoutes ~> check {
      status shouldBe Created
    }

    Put(s"/providers/$providerOneId/event-types/$eventTypeId", eventTypeBean) ~> eventTypeRoutes ~> check {
      status shouldBe Created
    }

    Get(s"/providers/$providerOneId/event-types/$eventTypeId/events") ~> eventsRoutes ~> check {
      status shouldBe OK
      contentType shouldBe `application/json`
      val content = responseAs[List[Event]]
      content should have length 0
    }

    Post(s"/providers/$providerOneId/event-types/$eventTypeId/events", new Event(None, "Bla", None)) ~> eventsRoutes ~> check {
      status shouldBe Created
      header("Location").get.value() should startWith(s"/providers/$providerOneId/event-types/$eventTypeId/events")
    }

    Get(s"/providers/$providerOneId/event-types/$eventTypeId/events") ~> eventsRoutes ~> check {
      status shouldBe OK
      contentType shouldBe `application/json`
      val content = responseAs[List[Event]]
      content should have length 1
    }

    Delete(s"/providers/$providerOneId/event-types/$eventTypeId/events") ~> eventsRoutes ~> check {
      status shouldBe NoContent
    }

    Get(s"/providers/$providerOneId/event-types/$eventTypeId/events") ~> eventsRoutes ~> check {
      status shouldBe OK
      contentType shouldBe `application/json`
      val content = responseAs[List[Event]]
      content should have length 0
    }

    Delete(s"/providers/$providerOneId/event-types/$eventTypeId") ~> eventTypeRoutes ~> check {
      status shouldBe NoContent
    }

    Get(s"/providers/$providerOneId/event-types/$eventTypeId/events") ~> eventsRoutes ~> check {
      status shouldBe NotFound
    }
  }

}