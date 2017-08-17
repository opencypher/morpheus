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
package org.opencypher.spark.impl.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{Partition, SparkConf, SparkContext, TaskContext}
import org.neo4j.driver.v1.Config
import org.neo4j.spark.Neo4j.NameProp
import org.neo4j.spark.{Neo4j, Neo4jConfig, Neo4jRDD, Partitions}

case class EncryptedNeo4j(session: SparkSession) extends Neo4j(session.sparkContext) {

  override def loadRelRdd : RDD[Row] = {
    if (pattern != null) {
      val queries: Seq[(String, List[String])] = pattern.relQueries
      val rdds: Seq[RDD[Row]] = queries.map(query => {
        //        val maxCountQuery: () => Long = () => { query._2.map(countQuery => new Neo4jRDD(sc, countQuery).first().getLong(0)).max }
        new EncryptedNeo4jRDD(sc, query._1, rels.params, partitions) // .copy(rowSource = Option(maxCountQuery)))
      })
      rdds.reduce((a, b) => a.union(b)).distinct()
    } else {
      new EncryptedNeo4jRDD(sc, rels.query, rels.params, partitions)
    }
  }

  override def loadNodeRdds : RDD[Row] = {
    if (pattern != null) {
      loadNodeRdds(pattern.source,nodes.params,partitions)
        .union(loadNodeRdds(pattern.target,nodes.params,partitions)).distinct()
    } else if (!nodes.isEmpty) {
      new EncryptedNeo4jRDD(sc, nodes.query, nodes.params, partitions)
    } else {
      null
    }
  }

  private def loadNodeRdds(node: NameProp, params: Map[String,Any], partitions : Partitions) = {
    // todo use count queries
    val queries = pattern.nodeQuery(node)

    new EncryptedNeo4jRDD(sc, queries._1, params, partitions)
  }

}

class EncryptedNeo4jRDD(sc: SparkContext, query: String, parameters: Map[String,Any] = Map.empty, partitions : Partitions = Partitions())
  extends Neo4jRDD(sc, query, parameters, partitions) {

  override val neo4jConfig: Neo4jConfig = EncryptedNeo4jConfig(sc.getConf)
}

class EncryptedNeo4jConfig(url: String, user: String, password: Option[String]) extends Neo4jConfig(url, user, password) {

  override def boltConfig() = Config.build.withEncryptionLevel(Config.EncryptionLevel.REQUIRED).toConfig

}

object EncryptedNeo4jConfig {
  val prefix = "spark.neo4j.bolt."
  def apply(sparkConf: SparkConf): EncryptedNeo4jConfig = {
    val url = sparkConf.get(prefix + "url", "bolt://localhost")
    val user = sparkConf.get(prefix + "user", "neo4j")
    val password: Option[String] = sparkConf.getOption(prefix + "password")
    new EncryptedNeo4jConfig(url, user, password)
  }
}
