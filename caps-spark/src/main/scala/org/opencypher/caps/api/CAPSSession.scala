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
package org.opencypher.caps.api

import java.util.{ServiceLoader, UUID}

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.opencypher.caps.api.graph.{CypherSession, PropertyGraph}
import org.opencypher.caps.api.schema.{Node, Relationship}
import org.opencypher.caps.demo.CypherKryoRegistrar
import org.opencypher.caps.impl.record.GraphScan.{nodesToScan, relationshipsToScan}
import org.opencypher.caps.impl.record.{CypherRecords, GraphScan, NodeScan}
import org.opencypher.caps.impl.spark._
import org.opencypher.caps.impl.spark.io.{CAPSGraphSourceHandler, CAPSPropertyGraphDataSourceFactory}

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._

// TODO: Move where they belong
trait CAPSEntitySchema
case class CAPSEntities(schema: CAPSEntitySchema, df: DataFrame)
case class CAPSNodeSchema(
  idColumn: String,
  impliedLabels: Set[String] = Set.empty,
  labelColumns: Set[String] = Set.empty,
  excludedColumns: Set[String] = Set.empty,
  optionalLabelColumns: Set[String] = Set.empty
) extends CAPSEntitySchema


trait CAPSSession extends CypherSession {

  def sparkSession: SparkSession

  /**
    * Reads a graph from sequences of nodes and relationships.
    *
    * @param nodes         sequence of nodes
    * @param relationships sequence of relationships
    * @tparam N node type implementing [[org.opencypher.caps.api.schema.Node]]
    * @tparam R relationship type implementing [[org.opencypher.caps.api.schema.Relationship]]
    * @return graph defined by the sequences
    */
  def readFrom[N <: Node : TypeTag, R <: Relationship : TypeTag](
    nodes: Seq[N],
    relationships: Seq[R] = Seq.empty): PropertyGraph = {
    implicit val session: CAPSSession = this
    CAPSGraph.create(nodesToScan(nodes), relationshipsToScan(relationships))
  }

  def readFromScans(nodeScan: NodeScan, scans: GraphScan*): PropertyGraph =
    CAPSGraph.create(nodeScan, scans: _*)(this)

//    def toNodeScan(df: DataFrame): NodeScan = {
//      val allColumns = df.columns.toSet
//      val propertyColumns = allColumns -- labelColumns -- excludedColumns -- optionalLabelColumns
//      NodeScan
//        .on("___some_leaked_internal_var_name" -> idColumn) { builder =>
//          builder.build.
//            withProperties(propertyColumns).
//            withImpliedLabels(impliedLabels.toArray: _*).
//            withOptionalLabels(optionalLabelColumns.toArray: _*)
//        }
//        .fromDf(df)
//    }
}

object CAPSSession extends Serializable {

  /**
    * Creates a new CAPSSession that wraps a local Spark session with CAPS default parameters.
    */
  def local(): CAPSSession = {
    val conf = new SparkConf(true)
    conf.set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
    conf.set("spark.kryo.registrator", classOf[CypherKryoRegistrar].getCanonicalName)
    conf.set("spark.sql.codegen.wholeStage", "true")
    conf.set("spark.kryo.unsafe", "true")
    conf.set("spark.kryo.referenceTracking", "false")
    conf.set("spark.kryo.registrationRequired", "true")

    val session = SparkSession
      .builder()
      .config(conf)
      .master("local[*]")
      .appName(s"caps-local-${UUID.randomUUID()}")
      .getOrCreate()
    session.sparkContext.setLogLevel("error")

    create(session)
  }

  def create(implicit session: SparkSession): CAPSSession = Builder(session).build

  case class Builder(
      session: SparkSession,
      private val graphSourceFactories: Set[CAPSPropertyGraphDataSourceFactory] = Set.empty) {

    def withGraphSourceFactory(factory: CAPSPropertyGraphDataSourceFactory): Builder =
      copy(graphSourceFactories = graphSourceFactories + factory)

    def build: CAPSSession = {
      val discoveredFactories = ServiceLoader.load(classOf[CAPSPropertyGraphDataSourceFactory]).asScala.toSet
      val allFactories = graphSourceFactories ++ discoveredFactories

      new CAPSSessionImpl(
        session,
        CAPSGraphSourceHandler(allFactories)
      )
    }
  }

  def builder(sparkSession: SparkSession): Builder = Builder(sparkSession)
}
