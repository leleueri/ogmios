package org.ogmios.core.actor

import scala.collection.immutable.Map
import scala.concurrent.Await
import scala.concurrent.TimeoutException
import scala.concurrent.duration.DurationInt
import scala.util.Success
import org.ogmios.core.action.Read
import org.ogmios.core.action.Register
import org.ogmios.core.action.Update
import org.ogmios.core.bean.OpResult
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
import org.ogmios.core.bean.Metric
import org.ogmios.core.bean.Event

class TestProviderActor(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
with WordSpecLike with Matchers with BeforeAndAfterAll with ActorNames {

  // TODO read scalatest documentation to improve following tests
  
    def this() = this(ActorSystem("TestProviderActor"))

    override def afterAll {
        TestKit.shutdownActorSystem(system)
    }

    val cassandra = system.actorOf(Props[CassandraActor], cassandraActor)
            
    "A Cassandra Actor" must {
        "send back OK on successful registration" in {
           val msg1 = new Register[Provider](new Provider("a-provider-id"+System.currentTimeMillis(), "MyProviderName", System.currentTimeMillis(), None))
          
           val future = ask(cassandra, msg1)(10.second);
           Await.ready(future, 10.second)
           future.value match {
             case Some(s: Success[Status]) => s.get.state shouldBe Status.StateOk
             case _ => fail
           }
        }
    }
    
    "A Cassandra Actor" must {
        "send back OK on successful metrics registration" in {
          val providerId = "prov-metrics-id"+System.currentTimeMillis()
          val msg1 = new Register[Provider](new Provider(providerId, "MyProviderName", System.currentTimeMillis(), None))
          val msg2 = new Register[Metric](new Metric(providerId, name = "mName", emission = System.currentTimeMillis(), value = 85.987))
          
           val future = ask(cassandra, msg1)(10.second);
           Await.ready(future, 10.second)
           future.value match {
             case Some(s: Success[Status]) => s.get.state shouldBe Status.StateOk
             case _ => fail
           }
           
           val futureM = ask(cassandra, msg2)(10.second);
           Await.ready(futureM, 10.second)
           futureM.value match {
             case Some(s: Success[Status]) => s.get.state shouldBe Status.StateOk
             case _ => fail
           }
        }
    }
    
       
    "A Cassandra Actor" must {
        "send back OK on successful events registration" in {
          val map = Map ("a"->"a", "c" -> "c")
          val providerId = "prov-metrics-id"+System.currentTimeMillis()
          val msg1 = new Register[Provider](new Provider(providerId, "MyProviderName", System.currentTimeMillis(), None))
          val msg2 = new Register[Event](new Event(providerId, name = "mName", emission = System.currentTimeMillis(), properties = map))
          
           val future = ask(cassandra, msg1)(10.second);
           Await.ready(future, 10.second)
           future.value match {
             case Some(s: Success[Status]) => s.get.state shouldBe Status.StateOk
             case _ => fail
           }
           
           val futureM = ask(cassandra, msg2)(10.second);
           Await.ready(futureM, 10.second)
           futureM.value match {
             case Some(s: Success[Status]) => s.get.state shouldBe Status.StateOk
             case _ => fail
           }
        }
    }
    
    "A Cassandra Actor" must {
        "send back OK on successful registration with Ref map" in {
           val map = Map ("a"->"a", "c" -> "c")
           val msg1 = new Register[Provider](new  Provider("a-provider-id"+System.currentTimeMillis(), "MyProviderName", System.currentTimeMillis(), Some(map)))
          
           val future = ask(cassandra, msg1)(10.second);
           Await.ready(future, 10.second)
           future.value match {
             case Some(s: Success[Status]) => s.get.state shouldBe Status.StateOk
             case _ => fail
           }
        }
    }
     
    "A Cassandra Actor" can {
            "not send back OK on a second registration with same provider id" in {
               val map = Map ("a"->"a", "c" -> "c")
               val providerId = "a-provider-id"+System.currentTimeMillis()
               val msg1 = new Register[Provider](new  Provider(providerId, "MyProviderName", System.currentTimeMillis(), Some(map)))
              
               val future = ask(cassandra, msg1)(10.second);
               Await.ready(future, 10.second)
               future.value match {
                 case Some(s: Success[Status]) =>  s.get.state shouldBe Status.StateOk
                 case _ => fail
               }
               
               val futureKO = ask(cassandra, msg1)(10.second);
               Await.ready(futureKO, 10.second)
               futureKO.value match {
                 case Some(s: Success[Status]) =>  s.get.state shouldBe Status.StateConflict
                 case _ => fail
               }
            }
    }
    
    "A Cassandra Actor" must {
        "read provider description" in {
            val id = "a-provider-id4read-"+System.currentTimeMillis()
            
            // create the object
            val map = Map ("a"->"a", "c" -> "c")
            val msg1 = new Register[Provider](new Provider(id, "MyProviderName", System.currentTimeMillis(), Some(map)))
            val future = ask(cassandra, msg1)(3600.second);
            Await.ready(future, 10.second)
            future.value match {
              case Some(s: Success[Status]) =>  s.get.state shouldBe Status.StateOk
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
       
    "A Cassandra Actor" should {
        "not provide unknown provider" in {
            val id = "unknown"+System.currentTimeMillis()
           
            // read the object
            val futureGet = ask(cassandra, new Read[Provider](id))(10.second);
            Await.ready(futureGet, 10.second)
            futureGet.value match {
              case Some(s: Success[Status]) =>  s.get.state shouldBe Status.StateNotFound
              case _ => fail
            }
        }
    }
     
    "A Cassandra Actor" can {
            "not update an unknown provider " in {
               val map = Map ("a"->"a", "c" -> "c")
               val providerId = "update-id"+System.currentTimeMillis()
               val regMsg = new Register[Provider](new  Provider(providerId, "MyUpdatedProvider", System.currentTimeMillis(), None))
               val updMsg = new Update[Provider](new  Provider(providerId, "MyUpdatedProvider", System.currentTimeMillis(), Some(map)))
               
               // update an unknown provider
               val futureKO = ask(cassandra, updMsg)(10.second);
               Await.ready(futureKO, 10.second)
               futureKO.value match {
                 case Some(s: Success[Status]) =>  s.get.state shouldBe Status.StateNotFound
                 case _ => fail
               }
               // create the provider
               val future = ask(cassandra, regMsg)(10.second);
               Await.ready(future, 10.second)
               future.value match {
                 case Some(s: Success[Status]) =>  s.get.state shouldBe Status.StateOk
                 case _ => fail
               }
               // update the provider
               val futureOK = ask(cassandra, updMsg)(10.second);
               Await.ready(futureOK, 10.second)
               futureOK.value match {
                 case Some(s: Success[Status]) =>  s.get.state shouldBe Status.StateOk
                 case _ => fail
               }
            }
    }
}