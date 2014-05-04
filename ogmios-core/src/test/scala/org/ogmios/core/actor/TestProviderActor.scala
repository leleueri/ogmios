package org.ogmios.core.actor

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.util.Success
import org.ogmios.core.action.Read
import org.ogmios.core.action.Register
import org.ogmios.core.bean.Provider
import org.ogmios.core.bean.Status
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import akka.actor.ActorSystem
import akka.actor.Props
import akka.pattern.ask
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import akka.util.Timeout.durationToTimeout
import scala.collection.immutable.Map
import org.ogmios.core.bean.OpResult
import org.ogmios.core.bean.OpResult

class TestAnotherProviderActor(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
with WordSpecLike with Matchers with BeforeAndAfterAll with ActorNames {

    def this() = this(ActorSystem("TestProviderActor"))

    override def afterAll {
        TestKit.shutdownActorSystem(system)
    }

    val cassandra = system.actorOf(Props[CassandraActor], cassandraActor)
            
    "A Provider Actor " must {

        "send back OK on successful registration" in {
           val msg1 = new Register[Provider](new Provider("a-provider-id"+System.currentTimeMillis(), "MyProviderName", System.currentTimeMillis(), None))
          
           val future = ask(cassandra, msg1)(10.second);
           Await.ready(future, 10.second)
           future.value match {
             case Some(s: Success[Status]) =>  assert(s.get.state == "OK")
             case _ => fail
           }
        }
    }
    
    "A Provider Actor " must {

        "send back OK on successful registration with Ref map" in {
           val map = Map ("a"->"a", "c" -> "c")
           val msg1 = new Register[Provider](new  Provider("a-provider-id"+System.currentTimeMillis(), "MyProviderName", System.currentTimeMillis(), Some(map)))
          
           val future = ask(cassandra, msg1)(10.second);
           Await.ready(future, 10.second)
           future.value match {
             case Some(s: Success[Status]) =>  assert(s.get.state == "OK")
             case _ => fail
           }
        }
    }
    
    "A Provider Actor " must {

        "read provider description" in {
            val id = "a-provider-id4read-"+System.currentTimeMillis()
            
            // create the object
            val msg1 = new Register[Provider](new Provider(id, "MyProviderName", System.currentTimeMillis(), None))
            val future = ask(cassandra, msg1)(10.second);
            Await.ready(future, 10.second)
            future.value match {
              case Some(s: Success[Status]) =>  assert(s.get.state == "OK")
              case _ => fail
            }
            
            // read the object
            val futureGet = ask(cassandra, new Read[Provider](id))(10.second);
            Await.ready(futureGet, 10.second)
            futureGet.value match {
              case Some(s: Success[OpResult[Provider]]) =>  assert(s.get.value.id === id)
              case _ => fail
            }
        }
    }
    
       
    "A Provider Actor " should {

        "not provide unknown provider" in {
            val id = "unknown"+System.currentTimeMillis()
           
            // read the object
            val futureGet = ask(cassandra, new Read[Provider](id))(10.second);
            Await.ready(futureGet, 10.second)
            futureGet.value match {
              case Some(s: Success[Status]) =>  assert(s.get.state == "NOT_FOUND")
              case _ => fail
            }
        }
    }
}