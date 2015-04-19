package com.github.leleueri.ogmios

import akka.event.NoLogging
import akka.http.marshallers.sprayjson.SprayJsonSupport._
import akka.http.model.ContentTypes._
import akka.http.model.{FormData, HttpResponse, HttpRequest}
import akka.http.model.StatusCodes._
import akka.http.testkit.ScalatestRouteTest
import akka.http.util.DateTime
import akka.stream.scaladsl.Flow
import com.github.leleueri.ogmios.OgmiosApp._
import com.github.leleueri.ogmios.protocol.Provider
import org.scalatest._
import spray.json.DefaultJsonProtocol

class TestProviderService extends FlatSpec with Matchers with ScalatestRouteTest with ProviderService with BeforeAndAfterEach {

  override def testConfigSource = "akka.loglevel = DEBUG"
  override def config = testConfig
  override val logger = NoLogging

  val keyspace = "ogmios"
  lazy val session = cluster.connect(keyspace)

  val providerOneId = "providerid-"+System.currentTimeMillis()
  val providerOneBean = new Provider(providerOneId, "name-"+providerOneId, None, Set.empty, None)

  override def afterEach() {
    // after each test case, remove the provider from cassandra
    session.execute(deleteStmt.bind().setString(COL_ID, providerOneId))
  }

  "Service" should "respond NOT_FOUND on missing providers" in {
    Get(s"/providers/unknownProvider") ~> providerRoutes ~> check {
      status shouldBe NotFound
    }
  }

  it should "respond BAD_REQUEST if provideId are inconsistent on PUT request" in {
    Put(s"/providers/nothesame", providerOneBean) ~> providerRoutes ~> check {
      status shouldBe BadRequest
    }
  }

  it should "respond CREATED on creation of a new provider and a GET should return the provider description" in {
    Put(s"/providers/$providerOneId", providerOneBean) ~> providerRoutes ~> check {
      status shouldBe Created
    }

    Get(s"/providers/$providerOneId") ~> providerRoutes ~> check {
      status shouldBe OK
      contentType shouldBe `application/json`
      val responseProvider: Provider = responseAs[Provider]
      responseProvider shouldBe a [Provider]

      responseProvider.creationDate should not be empty
      responseProvider.desc shouldBe empty
      responseProvider.name shouldBe providerOneBean.name
      responseProvider.id shouldBe providerOneBean.id
      responseProvider.eventTypes shouldBe empty
    }
  }

  it should "respond CREATED on creation of a new provider (with all fields)" in {

    val provider = new Provider(providerOneId, "name-"+providerOneId, Some("a description"), Set("TestEvent"), Some(DateTime.now))

    Put(s"/providers/$providerOneId", provider) ~> providerRoutes ~> check {
      status shouldBe Created
    }

    Get(s"/providers/$providerOneId") ~> providerRoutes ~> check {
      status shouldBe OK
      contentType shouldBe `application/json`
      val responseProvider: Provider = responseAs[Provider]
      responseProvider shouldBe a [Provider]

      responseProvider.creationDate shouldBe provider.creationDate
      responseProvider.desc shouldBe provider.desc
      responseProvider.name shouldBe provider.name
      responseProvider.id shouldBe provider.id
      responseProvider.eventTypes shouldBe provider.eventTypes
    }
  }

  it should "respond CONFLICT on creation of an existing provider " in {
    Put(s"/providers/$providerOneId", providerOneBean) ~> providerRoutes ~> check {
      status shouldBe Created
    }

    Put(s"/providers/$providerOneId", providerOneBean) ~> providerRoutes ~> check {
      status shouldBe Conflict
    }
  }

  it should "respond BAD_REQUEST if provideId are inconsistent on POST request" in {
    Post(s"/providers/nothesame", providerOneBean) ~> providerRoutes ~> check {
      status shouldBe BadRequest
    }
  }

  it should "respond NOT_FOUND on GET request after a delete" in {
    Put(s"/providers/$providerOneId", providerOneBean) ~> providerRoutes ~> check {
      status shouldBe Created
    }

    Get(s"/providers/$providerOneId") ~> providerRoutes ~> check {
      status shouldBe OK
      contentType shouldBe `application/json`
      val responseProvider: Provider = responseAs[Provider]
      responseProvider shouldBe a [Provider]
      responseProvider.id shouldBe providerOneBean.id
    }

    Delete(s"/providers/$providerOneId") ~> providerRoutes ~> check {
      status shouldBe NoContent
    }

    Get(s"/providers/$providerOneId") ~> providerRoutes ~> check {
      status shouldBe NotFound
    }
  }

  it should "respond NOT_CONTENT on DELETE if the provider doesn't exist" in {
    Get(s"/providers/$providerOneId") ~> providerRoutes ~> check {
      status shouldBe NotFound
    }

    Delete(s"/providers/$providerOneId") ~> providerRoutes ~> check {
      status shouldBe NoContent
    }
  }

  it should "respond OK on update (but only Name and Description fields are updatetable)" in {
    Put(s"/providers/$providerOneId", providerOneBean) ~> providerRoutes ~> check {
      status shouldBe Created
    }

    Get(s"/providers/$providerOneId") ~> providerRoutes ~> check {
      status shouldBe OK
      contentType shouldBe `application/json`
      val responseProvider: Provider = responseAs[Provider]
      responseProvider shouldBe a [Provider]

      responseProvider.creationDate should not be empty
      responseProvider.desc shouldBe empty
      responseProvider.name shouldBe providerOneBean.name
      responseProvider.id shouldBe providerOneBean.id
      responseProvider.eventTypes shouldBe empty
    }

    val providerUpdated = providerOneBean.copy(name ="UpdatedName", desc = Some("DescriptionUpdated"), eventTypes = Set("Test"))

    Post(s"/providers/$providerOneId", providerUpdated) ~> providerRoutes ~> check {
      status shouldBe OK
    }

    Get(s"/providers/$providerOneId") ~> providerRoutes ~> check {
      status shouldBe OK
      contentType shouldBe `application/json`
      val responseProvider: Provider = responseAs[Provider]
      responseProvider shouldBe a [Provider]

      responseProvider.creationDate should not be empty
      responseProvider.desc shouldBe providerUpdated.desc
      responseProvider.name shouldBe providerUpdated.name
      responseProvider.eventTypes shouldBe empty
    }
  }
}