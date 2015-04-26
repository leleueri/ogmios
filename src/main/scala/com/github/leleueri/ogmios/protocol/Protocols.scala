package com.github.leleueri.ogmios.protocol


import akka.http.marshallers.sprayjson.SprayJsonSupport
import akka.http.util.DateTime
import spray.json._

/**
 * Created by eric on 24/03/15.
 */


/**
 * Describes a provider of events.
 *
 * @param id
 * @param name
 * @param desc
 * @param eventTypes
 */
case class Provider(id: String, name: String, desc: Option[String], eventTypes : Set[String], creationDate: Option[DateTime])

/**
 *
 * @param id
 * @param unit
 * @param valueType
 * @param provider
 * @param creationDate
 */
case class EventType(id: String, unit: Option[String], valueType: String, provider: String, creationDate: Option[DateTime])

/**
 *
 * @param id
 * @param value
 * @param creationDate
 */
case class Event(id: Option[String], value: String, creationDate: Option[DateTime])

case class Error(category: String, message: String)

trait Protocols extends DefaultJsonProtocol with SprayJsonSupport { // with SpryJsonSupport to manage List of EventType...
  import akka.http.marshallers.sprayjson._

  // define a DateTime Json Marshaller/Unmarshaller
  // DO NOT use the RootJsonFormat because, DateTime will be convert as String
  implicit object OptDateTimeJsonFormat extends JsonFormat[DateTime] {
    def write(d: DateTime) = JsString(d.toIsoDateTimeString())  //JsString(d.toIsoDateTimeString())
    def read(value: JsValue) = DateTime.fromIsoDateTimeString(value.convertTo[String]).orNull
  }

  implicit val providerJson = jsonFormat5(Provider.apply)
  implicit val eventTypeJson = jsonFormat5(EventType.apply)
  implicit val eventJson = jsonFormat3(Event.apply)
  implicit val errorJson = jsonFormat2(Error.apply)
}
