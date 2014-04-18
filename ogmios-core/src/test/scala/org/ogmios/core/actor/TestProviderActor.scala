package org.ogmios.core.actor

import akka.testkit.TestKit
import akka.testkit.ImplicitSender
import org.scalatest.WordSpecLike
import org.scalatest.Matchers
import org.scalatest.BeforeAndAfterAll
import akka.actor.ActorSystem
import akka.actor.Props
import org.ogmios.core.action.Register
import org.ogmios.core.bean.Provider
import java.util.Date
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

class TestAnotherProviderActor(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
with WordSpecLike with Matchers with BeforeAndAfterAll with ActorNames {

    def this() = this(ActorSystem("TestProviderActor"))

    override def afterAll {
        TestKit.shutdownActorSystem(system)
    }

    "A Provider Actor " must {

        "send back OK on successful registration" in {
            val provider = system.actorOf(Props[ProviderActor], providerActor)
            provider ! new Register[Provider](new Provider("a-provider-id"+System.currentTimeMillis(), "MyProviderName", new Date, None))
            provider ! new Register[Provider](new Provider("a-provider-id"+System.currentTimeMillis()+1, "MyProviderName", new Date, None))
            val seqResp = receiveN(2, 10.seconds)
            assert(2 === seqResp.count(_ == "OK"))
        }

    }
}