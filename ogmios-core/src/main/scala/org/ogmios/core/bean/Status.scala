package org.ogmios.core.bean

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


trait Status {
  def state : String
  def msg : String
}


case class OpCompleted(val state: String, val msg : String) extends Status
case class OpFailed(val state: String, val msg : String) extends Status
case class OpResult[T](val state: String, val msg : String, val value: T) extends Status