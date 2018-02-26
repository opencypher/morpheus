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
package org.opencypher.spark.api

import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.opencypher.okapi.api.graph.{CypherSession, Namespace, PropertyGraph}
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.okapi.impl.io.SessionPropertyGraphDataSource
import org.opencypher.okapi.relational.impl.table.ColumnName
import org.opencypher.spark.api.io._
import org.opencypher.spark.impl.{CAPSGraph, CAPSRecords, CAPSSessionImpl, CypherKryoRegistrator}

import scala.reflect.runtime.universe._

trait CAPSSession extends CypherSession {

  def sql(query: String): CypherRecords

  def sparkSession: SparkSession

  /**
    * Reads a graph from sequences of nodes and relationships.
    *
    * @param nodes         sequence of nodes
    * @param relationships sequence of relationships
    * @tparam N node type implementing [[Node]]
    * @tparam R relationship type implementing [[Relationship]]
    * @return graph defined by the sequences
    */
  def readFrom[N <: Node : TypeTag, R <: Relationship : TypeTag](
    nodes: Seq[N],
    relationships: Seq[R] = Seq.empty): PropertyGraph = {
    implicit val session: CAPSSession = this
    CAPSGraph.create(CAPSNodeTable(nodes), CAPSRelationshipTable(relationships))
  }

  /**
    * Reads a graph from a sequence of entity tables and expects that the first table is a node table.
    *
    * @param entityTables sequence of node and relationship tables defining the graph
    * @return property graph
    */
  def readFrom(entityTables: CAPSEntityTable*): PropertyGraph = entityTables.head match {
    case h: CAPSNodeTable => readFrom(h, entityTables.tail: _*)
    case _ => throw IllegalArgumentException("first argument of type NodeTable", "RelationshipTable")
  }

  /**
    * Reads a graph from a sequence of entity tables that contains at least one node table.
    *
    * @param nodeTable    first parameter to guarantee there is at least one node table
    * @param entityTables sequence of node and relationship tables defining the graph
    * @return property graph
    */
  def readFrom(nodeTable: CAPSNodeTable, entityTables: CAPSEntityTable*): PropertyGraph = {
    CAPSGraph.create(nodeTable, entityTables: _*)(this)
  }
}

object CAPSSession extends Serializable {

  /**
    * Creates a new [[CAPSSession]] based on the given [[SparkSession]].
    *
    * @param sparkSession Spark session
    * @return CAPS session
    */
  def create(sessionNamespace: Namespace = SessionPropertyGraphDataSource.Namespace)
    (implicit sparkSession: SparkSession): CAPSSession = new CAPSSessionImpl(sparkSession, sessionNamespace)

  /**
    * Creates a new CAPSSession that wraps a local Spark session with CAPS default parameters.
    */
  def local(settings: (String, String)*): CAPSSession = {
    val conf = new SparkConf(true)
//    conf.set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
//    conf.set("spark.kryo.registrator", classOf[CypherKryoRegistrator].getCanonicalName)
    conf.set("spark.sql.codegen.wholeStage", "true")
//    conf.set("spark.kryo.unsafe", "true")
//    conf.set("spark.kryo.referenceTracking", "false")
//    conf.set("spark.kryo.registrationRequired", "true")
    conf.set("spark.sql.shuffle.partitions", "12")
    conf.set("spark.default.parallelism", "8")
    conf.setAll(settings)

    val session = SparkSession
      .builder()
      .config(conf)
      .master("local[*]")
      .appName(s"caps-local-${UUID.randomUUID()}")
      .getOrCreate()
    session.sparkContext.setLogLevel("error")

    create()(session)
  }

  /**
    * Returns the DataFrame column name for the given Cypher RETURN item.
    *
    * {{{
    * import org.opencypher.caps.api.CAPSSession._
    * // ...
    * val results = socialNetwork.cypher("MATCH (a) RETURN a.name")
    * val dataFrame = results.records.asDF
    * val projection = dataFrame.select(columnFor("a.name"))
    * }}}
    *
    * @param returnItem Cypher RETURN item (e.g. "a.name")
    * @return DataFrame column name for given RETURN item
    */
  // TODO: Consider moving this to CypherRecords instead
  def columnFor(returnItem: String): String = ColumnName.from(returnItem)

  /**
    * Import this into scope in order to use:
    *
    * {{{
    * import org.opencypher.caps.api.CAPSSession.RecordsAsDF
    * // ...
    * val df: DataFrame = results.records.asDF
    * }}}
    */
  implicit class RecordsAsDF(val records: CypherRecords) extends AnyVal {
    /**
      * Extracts the underlying [[DataFrame]] from the given [[records]].
      *
      * Note that the column names in the returned DF do not necessarily correspond to the names of the Cypher RETURN
      * items, e.g. "RETURN n.name" does not mean that the column for that item is named "n.name". In order to get the
      * column name for a RETURN item, use [[columnFor]].
      *
      * @return [[DataFrame]] representing the records
      */
    def asDataFrame: DataFrame = records match {
      case caps: CAPSRecords => caps.data
      case _ => throw UnsupportedOperationException(s"can only handle CAPS records, got $records")
    }

    /**
      * Converts all values stored in this table to instances of the corresponding CypherValue class.
      * In particular, this de-flattens, or collects, flattened entities (nodes and relationships) into
      * compact CypherNode/CypherRelationship objects.
      *
      * All values on each row are inserted into a CypherMap object mapped to the corresponding field name.
      *
      * @return [[Dataset]] of CypherMaps
      */
    def asDataset: Dataset[CypherMap] = records match {
      case caps: CAPSRecords => caps.toCypherMaps
      case _ => throw UnsupportedOperationException(s"can only handle CAPS records, got $records")
    }
  }
}
