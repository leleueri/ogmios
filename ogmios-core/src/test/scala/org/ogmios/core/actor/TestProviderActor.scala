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
import org.ogmios.core.bean.Status
import org.ogmios.core.action.Read

class TestAnotherProviderActor(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
with WordSpecLike with Matchers with BeforeAndAfterAll with ActorNames {

    def this() = this(ActorSystem("TestProviderActor"))

    override def afterAll {
        TestKit.shutdownActorSystem(system)
    }

    val provider = system.actorOf(Props[ProviderActor], providerActor)
            
    "A Provider Actor " must {

        "send back OK on successful registration" in {
            provider ! new Register[Provider](new Provider("a-provider-id"+System.currentTimeMillis(), "MyProviderName", new Date, None))
            provider ! new Register[Provider](new Provider("a-provider-id"+System.currentTimeMillis()+1, "MyProviderName", new Date, None))
            val seqResp = receiveN(2, 10.seconds)
            assert(2 === seqResp.count((x: AnyRef) => {  x.asInstanceOf[Status].state == "OK"}))
        }

    }
    
    "A Provider Actor " must {

        "read provider description" in {
            val id = "a-provider-id4read-"+System.currentTimeMillis()
            
            // create the object
            provider ! new Register[Provider](new Provider(id, "MyProviderName", new Date, None))
            val createResp = receiveN(1, 5.seconds)
           
            // read the object
            provider ! new Read[Provider](id)
            val seqResp = receiveN(1, 5.seconds)
            assert(id === (seqResp.head).asInstanceOf[Provider].id)
        }
    }
    
       
    "A Provider Actor " should {

        "not provide unknown provider" in {
            val id = "unknown"+System.currentTimeMillis()
           
            // read the object
            provider ! new Read[Provider](id)
            val seqResp = receiveN(1, 5.seconds)
            assert("NOT_FOUND" === (seqResp.head).asInstanceOf[Status].state)
        }
    }
}