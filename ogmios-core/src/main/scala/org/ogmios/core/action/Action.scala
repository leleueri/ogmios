package org.ogmios.core.action

import org.ogmios.core.bean.MetricDesc
import akka.actor.ActorRef

/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
trait Action

case class Register[T](element: T) extends Action
case class RegisterFrom[T](element: T, sender: ActorRef) extends Action

case class AddMetrics(name: String, metrics: List[MetricDesc]) extends Action

case class Read[T](id: String) extends Action

