package com.github.leleueri.ogmios

import akka.event.{Logging, NoLogging}
import akka.http.model.ContentTypes._
import akka.http.model.StatusCodes._
import akka.http.testkit.ScalatestRouteTest
import akka.http.util.DateTime
import com.github.leleueri.ogmios.OgmiosApp._
import com.github.leleueri.ogmios.protocol.{EventType, Provider}
import org.scalatest._

class TestEventTypeService extends FlatSpec with Matchers with ScalatestRouteTest with ProviderService with BeforeAndAfterEach {

  override def testConfigSource = "akka.loglevel = DEBUG"
  override def config = testConfig
  override val logger = NoLogging

  val keyspace = "ogmios"
  lazy val session = cluster.connect(keyspace)

  val providerOneId = "providerid-inevent-"+System.currentTimeMillis()
  val providerOneBean = new Provider(providerOneId, "name-"+providerOneId, None, Set.empty, None)

  val firstEventId: String = "firstEventId" + System.currentTimeMillis()
  val secondEventId: String = "secondEventId" + System.currentTimeMillis()
  val firstEvent = new EventType(firstEventId, None, "int", providerOneId, None)
  val secondEvent = new EventType(secondEventId, Some("ms"), "int", providerOneId, None)

  override def afterEach() {
    // after each test case, remove the provider from cassandra
    session.execute(deleteProvStmt.bind().setString(COL_PROV_ID, providerOneId))

    // after each test case, remove all types of event fo the provider from cassandra
    session.execute(deleteAllTypesStmt.bind().setString(COL_EVT_TYPE_PROV, providerOneId))
  }

  "Service" should "respond NOT_FOUND on unknown provider" in {
    Get(s"/providers/providerid_unknown/event-types") ~> allEventTypeRoutes ~> check {
      status shouldBe NotFound
    }
  }

  it should "respond OK on empty list of eventType " in {
    Put(s"/providers/$providerOneId", providerOneBean) ~> providerRoutes ~> check {
      status shouldBe Created
    }

    Get(s"/providers/$providerOneId/event-types") ~> allEventTypeRoutes ~> check {
      status shouldBe OK
      contentType shouldBe `application/json`
      val content = responseAs[List[EventType]]
      content isEmpty
    }
  }

  it should "respond BAD_REQUEST if provideId are inconsistent on PUT request" in {
    Put(s"/providers/$providerOneId", providerOneBean) ~> providerRoutes ~> check {
      status shouldBe Created
    }

    Put(s"/providers/$providerOneId/event-types/$firstEventId", firstEvent.copy(provider = "testOtherId")) ~> eventTypeRoutes ~> check {
      status shouldBe BadRequest
    }
  }

  it should "respond BAD_REQUEST if eventTypeId are inconsistent on PUT request" in {
    Put(s"/providers/$providerOneId", providerOneBean) ~> providerRoutes ~> check {
      status shouldBe Created
    }

    Put(s"/providers/$providerOneId/event-types/$firstEventId", firstEvent.copy(id = "testOtherId")) ~> eventTypeRoutes ~> check {
      status shouldBe BadRequest
    }
  }

  it should "respond CREATED eventType is created and a list All EventType should return a non empty list" in {
    Put(s"/providers/$providerOneId", providerOneBean) ~> providerRoutes ~> check {
      status shouldBe Created
    }

    Put(s"/providers/$providerOneId/event-types/$firstEventId", firstEvent) ~> eventTypeRoutes ~> check {
      status shouldBe Created
    }

    Put(s"/providers/$providerOneId/event-types/$secondEventId", secondEvent) ~> eventTypeRoutes ~> check {
      status shouldBe Created
    }

    Get(s"/providers/$providerOneId/event-types") ~> allEventTypeRoutes ~> check {
      status shouldBe OK
      contentType shouldBe `application/json`
      val content = responseAs[List[EventType]]
      content should have length 2
    }

    Delete(s"/providers/$providerOneId/event-types") ~> allEventTypeRoutes ~> check {
      status shouldBe NoContent
    }

    Get(s"/providers/$providerOneId/event-types") ~> allEventTypeRoutes ~> check {
      status shouldBe OK
      contentType shouldBe `application/json`
      val content = responseAs[List[EventType]]
      content isEmpty
    }
  }

}