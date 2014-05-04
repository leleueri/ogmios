package io.ogmios.rest

import org.specs2.mutable.Specification
import spray.testkit.Specs2RouteTest
import spray.http._
import StatusCodes._

class ProviderSpec extends Specification with Specs2RouteTest with OgmiosService {
  def actorRefFactory = system
  /*
  "OgmiosService" should {

    "return an instance of Provider on GET request to the provider path" in {
      Get("/providers/") ~> ogmiosRoute ~> check {
        responseAs[String] must contain("Say hello")
      }
    }

    "leave GET requests to other paths unhandled" in {
      Get("/kermit") ~> ogmiosRoute ~> check {
        handled must beFalse
      }
    }

    "return a MethodNotAllowed error for HEAD requests to the root path" in {
      Put() ~> sealRoute(ogmiosRoute) ~> check {
        status === MethodNotAllowed
        responseAs[String] === "HTTP method not allowed, supported methods: GET"
      }
    }
  }*/
}
