package com.github.leleueri.ogmios.protocol

import java.util.UUID

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
case class Provider(val id: String, val name: String, val desc: Option[String], val eventTypes : Set[String], val creationDate: Option[DateTime])

/**
 *
 * @param id
 * @param name
 * @param unit
 * @param valueType
 * @param provider
 * @param creationDate
 */
case class EventType(val id: String, val name: String, val unit: Option[String], val valueType: String, val provider: String, val creationDate: Option[DateTime])

/**
 *
 * @param evtType
 * @param id
 * @param value
 * @param creationDate
 */
case class Event(val evtType: String, val id: Option[String], val value: String, val creationDate: Option[Long])

trait Protocols extends DefaultJsonProtocol {

  // define a DateTime Json Marshaller/Unmarshaller
  // DO NOT use the RootJsonFormat because, DateTime will be convert as String
  implicit object OptDateTimeJsonFormat extends JsonFormat[DateTime] {
    def write(d: DateTime) = JsString(d.toIsoDateTimeString())  //JsString(d.toIsoDateTimeString())
    def read(value: JsValue) = DateTime.fromIsoDateTimeString(value.convertTo[String]).orNull
  }

  implicit val providerJson = jsonFormat5(Provider.apply)
  implicit val eventTypeJson = jsonFormat6(EventType.apply)
  implicit val eventJson = jsonFormat4(Event.apply)
}
