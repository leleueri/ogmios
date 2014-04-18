package org.ogmios.core.bean

/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

trait Message{

  def provider: String

  def emission: Long

  def reception: Long
}

/**
 *
 * @param provider the name/identifier of the data provider. This provider reference a profile that is registered in the system
 * @param emission the emission date of the metrics by the provider
 * @param reception the reception date of the metrics by the ogmios server
 * @param name name of the metric. This name must be defined in the provider profile
 * @param value value of the metric (value must be interpreted according to the Unit defined in the provider profile for this metrics name
 */
case class Metric(provider:String, emission: Long, reception: Long, name:String, value: String) extends Message





class MetricDesc(name: String, unit: String, desc: Option[String])