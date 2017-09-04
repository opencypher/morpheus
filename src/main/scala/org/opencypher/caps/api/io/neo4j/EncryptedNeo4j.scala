/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
 *
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
package org.opencypher.caps.api.io.neo4j

import java.net.URI

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.neo4j.driver.v1.Config
import org.neo4j.spark.{Neo4j, Neo4jConfig, Neo4jRDD, Partitions}

case class EncryptedNeo4j(config: EncryptedNeo4jConfig, session: SparkSession) extends Neo4j(session.sparkContext) {

  override def loadRelRdd : RDD[Row] = {
    if (pattern != null) {
      val queries: Seq[(String, List[String])] = pattern.relQueries
      val rdds: Seq[RDD[Row]] = queries.map(query => {
        new EncryptedNeo4jRDD(sc, query._1, config, rels.params, partitions)
      })
      rdds.reduce((a, b) => a.union(b)).distinct()
    } else {
      new EncryptedNeo4jRDD(sc, rels.query, config, rels.params, partitions)
    }
  }

  override def loadNodeRdds : RDD[Row] = {
    if (pattern != null) {
      loadNodeRdds(pattern.source,nodes.params,partitions)
        .union(loadNodeRdds(pattern.target,nodes.params,partitions)).distinct()
    } else if (!nodes.isEmpty) {
      new EncryptedNeo4jRDD(sc, nodes.query, config, nodes.params, partitions)
    } else {
      null
    }
  }

  private def loadNodeRdds(node: NameProp, params: Map[String, Any], partitions : Partitions) = {
    // todo use count queries
    val queries = pattern.nodeQuery(node)

    new EncryptedNeo4jRDD(sc, queries._1, config, params, partitions)
  }

}

class EncryptedNeo4jRDD(sc: SparkContext,
                        query: String,
                        override val neo4jConfig: EncryptedNeo4jConfig,
                        parameters: Map[String, Any] = Map.empty,
                        partitions: Partitions = Partitions())
  extends Neo4jRDD(sc, query, parameters, partitions)

class EncryptedNeo4jConfig(val uri: URI,
                           user: String = "",
                           password: Option[String] = None,
                           encryptionLevel: Config.EncryptionLevel = Config.EncryptionLevel.REQUIRED)
  extends Neo4jConfig(uri.toString, user, password) {

  override def boltConfig(): Config = Config.build.withEncryptionLevel(encryptionLevel).toConfig
}