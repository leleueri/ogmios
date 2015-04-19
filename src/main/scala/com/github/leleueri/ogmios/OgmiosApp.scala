package com.github.leleueri.ogmios

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.Http
import akka.stream.ActorFlowMaterializer
import com.typesafe.config.ConfigFactory
import akka.http.server.{Route, RouteConcatenation}

/**
 * Created by eric on 24/03/15.
 */
object OgmiosApp extends App with ProviderService with EventTypeService with RouteConcatenation {
  override implicit val system = ActorSystem()
  override implicit val executor = system.dispatcher
  override implicit val materializer = ActorFlowMaterializer()

  override val config = ConfigFactory.load()
  override val logger = Logging(system, getClass)

  val keyspace = "ogmios"
  lazy val session = cluster.connect(keyspace)

  val routes = allEventTypeRoutes ~ eventTypeRoutes ~ providerRoutes

  Http().bindAndHandle(interface = config.getString("http.interface"), port = config.getInt("http.port"), handler = routes)
}
