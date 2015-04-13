package com.github.leleueri.scalandra

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
// COPY/PASTE from the Akka/Cassandra activator
import com.datastax.driver.core.{ConsistencyLevel, ProtocolOptions, Cluster}
import akka.actor.ActorSystem
import com.typesafe.config.Config

trait CassandraCluster {
  def cluster: Cluster
}

trait ConfigCassandraCluster extends CassandraCluster {

  def keyspace: String

  def config: Config

  import scala.collection.JavaConversions._

  private lazy val cassandraConfig = config.getConfig("cassandra.cluster.connection")
  private lazy val port = cassandraConfig.getInt("port")
  private lazy val hosts = cassandraConfig.getStringList("hosts").toList

  private lazy val keyspaceConfig = config.getConfig("cassandra.cluster.keyspaces").getConfig(keyspace)

  lazy val cluster: Cluster =
    Cluster.builder().
      addContactPoints(hosts: _*).
      withPort(port).
      build()

  lazy val readConsistency: ConsistencyLevel = ConsistencyLevel.valueOf(keyspaceConfig.getString("readConsistency"))
  lazy val writeConsistency: ConsistencyLevel = ConsistencyLevel.valueOf(keyspaceConfig.getString("writeConsistency"))
}
