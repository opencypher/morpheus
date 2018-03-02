/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.spark.impl.io.neo4j.external

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.spark.api.io.neo4j.Neo4jConfig
import org.opencypher.spark.impl.io.neo4j.external.Neo4j._

import scala.reflect.ClassTag

object Neo4j {

  val UNDEFINED: Long = Long.MaxValue

  trait QueriesDsl {
    def cypher(cypher: String, params: Map[String, Any]): Neo4j

    def params(params: Map[String, Any]): Neo4j

    def param(key: String, value: Any): Neo4j

    def nodes(cypher: String, params: Map[String, Any]): Neo4j

    def rels(cypher: String, params: Map[String, Any]): Neo4j
  }

  trait PartitionsDsl {
    def partitions(partitions: Long): Neo4j

    def batch(batch: Long): Neo4j

    def rows(rows: Long): Neo4j
  }

  trait LoadDsl {
    def loadRdd[T: ClassTag]: RDD[T]

    def loadRowRdd: RDD[Row]

    def loadNodeRdds: RDD[Row]

    def loadRelRdd: RDD[Row]
  }

  case class Query(query: String, params: Map[String, Any] = Map.empty) {
    def paramsSeq: Seq[(String, Any)] = params.toSeq

    def isEmpty: Boolean = query == null
  }
}

case class Partitions(
    partitions: Long = 1,
    batchSize: Long = Neo4j.UNDEFINED,
    rows: Long = Neo4j.UNDEFINED,
    rowSource: Option[() => Long] = None) {

  def upper(v1: Long, v2: Long): Long = v1 / v2 + Math.signum(v1 % v2).asInstanceOf[Long]

  def effective(): Partitions = {
    if (this.batchSize == Neo4j.UNDEFINED && this.rows == Neo4j.UNDEFINED) return Partitions()
    if (this.batchSize == Neo4j.UNDEFINED) return this.copy(batchSize = upper(rows, partitions))
    if (this.rows == Neo4j.UNDEFINED) return this.copy(rows = this.batchSize * this.partitions)
    if (this.partitions == 1) return this.copy(partitions = upper(rows, batchSize))
    this
  }

  def skip(index: Int): Long = index * batchSize

  // if the last batch is smaller to fit the total rows
  def limit(index: Int): Long = {
    val remainder = rows % batchSize
    if (index < partitions - 1 || remainder == 0) batchSize else remainder
  }
}

case class Neo4j(config: Neo4jConfig, session: SparkSession) extends QueriesDsl with PartitionsDsl with LoadDsl {

  var nodes: Query = Query(null)
  var rels: Query = Query(null)

  var partitions = Partitions()

  // configure plain query

  override def cypher(cypher: String, params: Map[String, Any] = Map.empty): Neo4j = {
    this.nodes = Query(cypher, this.nodes.params ++ params)
    this
  }

  override def param(key: String, value: Any): Neo4j = {
    this.nodes = this.nodes.copy(params = this.nodes.params + (key -> value))
    this
  }

  override def params(params: Map[String, Any]): Neo4j = {
    this.nodes = this.nodes.copy(params = this.nodes.params ++ params)
    this
  }

  override def nodes(cypher: String, params: Map[String, Any]): Neo4j =
    this.cypher(cypher, params)

  override def rels(cypher: String, params: Map[String, Any] = Map.empty): Neo4j = {
    this.rels = Query(cypher, params)
    this
  }

  // configure partitions

  override def rows(rows: Long): Neo4j = {
    assert(rows > 0)
    this.partitions = partitions.copy(rows = rows)
    this
  }

  override def batch(batch: Long): Neo4j = {
    assert(batch > 0)
    this.partitions = partitions.copy(batchSize = batch)
    this
  }

  // todo for partitions > 1, generate a batched query SKIP {_skip} LIMIT {_limit}
  // batch could be hard-coded in query, so we only have to pass skip
  override def partitions(partitions: Long): Neo4j = {
    assert(partitions > 0)
    this.partitions = this.partitions.copy(partitions = partitions)
    this
  }

  // output

  override def loadNodeRdds: RDD[Row] =
    if (!nodes.isEmpty) {
      new Neo4jRDD(session.sparkContext, nodes.query, config, nodes.params, partitions)
    } else {
      throw IllegalArgumentException("node query")
    }

  override def loadRelRdd: RDD[Row] =
    if (!rels.isEmpty) {
      new Neo4jRDD(session.sparkContext, rels.query, config, rels.params, partitions)
    } else {
      throw IllegalArgumentException("relationship query")
    }

  override def loadRowRdd: RDD[Row] = loadNodeRdds

  override def loadRdd[T: ClassTag]: RDD[T] = loadRowRdd.map(_.getAs[T](0))
}
