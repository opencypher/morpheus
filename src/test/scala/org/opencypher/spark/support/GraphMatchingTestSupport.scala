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
package org.opencypher.spark.support

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.opencypher.spark.api.classes.Cypher
import org.opencypher.spark.api.expr.{HasLabel, Property, Var}
import org.opencypher.spark.api.ir.global.TokenRegistry
import org.opencypher.spark.api.record.{FieldSlotContent, RecordHeader}
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.spark.{SparkCypherGraph, SparkCypherRecords, SparkCypherResult, SparkGraphSpace}
import org.opencypher.spark.api.types.{CTNode, CTRelationship}
import org.opencypher.spark.api.value.{CypherMap, CypherValue}
import org.opencypher.spark.impl.convert.{fromJavaType, toSparkType}
import org.opencypher.spark.impl.physical.RuntimeContext
import org.opencypher.spark.impl.record.SparkCypherRecordsTokens
import org.opencypher.spark.{BaseTestSuite, SparkTestSession}
import org.s1ck.gdl.GDLHandler
import org.s1ck.gdl.model.Element
import org.scalatest.Assertion

import scala.collection.Bag
import scala.collection.immutable._
import scala.collection.JavaConverters._

trait GraphMatchingTestSupport {

  self: BaseTestSuite with SparkTestSession.Fixture  =>

  implicit val bagConfig = Bag.configuration.compact[CypherMap]
  val DEFAULT_LABEL = "DEFAULT"
  val sparkSession = session

  implicit val context: RuntimeContext = RuntimeContext.empty

  implicit class GraphMatcher(graph: SparkCypherGraph) {
    def shouldMatch(expectedGraph: SparkCypherGraph): Assertion = {
      val expectedNodeIds = expectedGraph.nodes("n").data.select("n").collect().map(_.getLong(0)).toSet
      val expectedRelIds = expectedGraph.relationships("r").data.select("r").collect().map(_.getLong(0)).toSet

      val actualNodeIds = graph.nodes("n").data.select("n").collect().map(_.getLong(0)).toSet
      val actualRelIds = graph.relationships("r").data.select("r").collect().map(_.getLong(0)).toSet

      expectedNodeIds should equal(actualNodeIds)
      expectedRelIds should equal(actualRelIds)
    }
  }

  case class TestGraph(gdl: String)(implicit sparkSession: SparkSession) {

    private val queryGraph = new GDLHandler.Builder()
      .setDefaultEdgeLabel(DEFAULT_LABEL)
      .setDefaultVertexLabel(DEFAULT_LABEL)
      .buildFromString(gdl)

    def cypher[C <: Cypher { type Graph = SparkCypherGraph; type Result = SparkCypherResult }]
      (query: String)(implicit engine: C)
    : SparkCypherResult =
      engine.cypher(graph, query)

    def cypher[C <: Cypher { type Graph = SparkCypherGraph; type Result = SparkCypherResult }]
      (query: String, parameters: Map[String, CypherValue])(implicit engine: C)
    : SparkCypherResult =
      engine.cypher(graph, query, parameters)

    lazy val graph: SparkCypherGraph = new SparkCypherGraph {
      self =>

      private def extractFromElement(e: Element) = e.getLabels.asScala.map { label =>
        label -> e.getProperties.asScala.map {
          case (name, prop) => name -> fromJavaType(prop)
        }
      }

      override val schema: Schema = {
        val labelAndProps = queryGraph.getVertices.asScala.flatMap(extractFromElement)
        val typesAndProps = queryGraph.getEdges.asScala.flatMap(extractFromElement)

        val schemaWithLabels = labelAndProps.foldLeft(Schema.empty) {
          case (acc, (label, props)) => acc.withNodePropertyKeys(label)(props.toSeq: _*)
        }

        typesAndProps.foldLeft(schemaWithLabels) {
          case (acc, (t, props)) => acc.withRelationshipPropertyKeys(t)(props.toSeq: _*)
        }
      }

      override val space: SparkGraphSpace = new SparkGraphSpace {
        override val session: SparkSession = sparkSession
        override var tokens: SparkCypherRecordsTokens = SparkCypherRecordsTokens(TokenRegistry.fromSchema(schema))
        override val base: SparkCypherGraph = {
          self
        }
      }

      override def nodes(name: String, cypherType: CTNode): SparkCypherRecords = {
        val header = RecordHeader.nodeFromSchema(Var(name)(cypherType), schema, space.tokens.registry,
          cypherType.labels.filter(_._2).keySet)

        val data = {
          val nodes = queryGraph.getVertices.asScala
            .filter(v => v.getLabels.containsAll(cypherType.labels.filter(_._2).keySet.asJava))
            .map { v =>
              val exprs = header.slots.map(_.content.key)
              val labelFields = exprs.collect {
                case HasLabel(_, label) => v.getLabels.contains(label.name)
              }
              val propertyFields = exprs.collect {
                case p@Property(_, k) =>
                  val pValue = v.getProperties.get(k.name)
                  if (fromJavaType(pValue) == p.cypherType) pValue
                  else null
              }

              Row(v.getId +: (labelFields ++ propertyFields): _*)
            }.toList.asJava

          val fields = header.slots.map { s =>
            StructField(context.columnName(s), toSparkType(s.content.cypherType))
          }

          sparkSession.createDataFrame(nodes, StructType(fields))
        }
        SparkCypherRecords.create(header, data)(space)
      }

      override def relationships(name: String, cypherType: CTRelationship): SparkCypherRecords = {

        val header = RecordHeader.relationshipFromSchema(Var(name)(cypherType), schema, space.tokens.registry)

        val data = {
          val rels = queryGraph.getEdges.asScala
            .filter(e => cypherType.types.asJava.isEmpty || cypherType.types.asJava.containsAll(e.getLabels))
            .map { e =>
              val staticFields = Seq(e.getSourceVertexId, e.getId, space.tokens.registry.relTypeRefByName(e.getLabel).id.toLong, e.getTargetVertexId)

              val propertyFields = header.slots.slice(4, header.slots.size).map(_.content.key).map {
                case Property(_, k) => e.getProperties.get(k.name)
                case _ => throw new IllegalArgumentException("Only properties expected in the header")
              }

              Row(staticFields ++ propertyFields: _*)
          }.toList.asJava

          val fields = header.slots.map { s =>
            StructField(context.columnName(s), toSparkType(s.content.cypherType))
          }

          sparkSession.createDataFrame(rels, StructType(fields))
        }
        SparkCypherRecords.create(header, data)(space)
      }
    }
  }

  // TODO: Move to RecordMatchingTestSupport
  implicit class RichRecords(records: SparkCypherRecords) {
    import org.opencypher.spark.impl.instances.spark.RowUtils._

    def toMaps: Bag[CypherMap] = {
      val rows = records.toDF().collect().map { r =>
        val properties = records.header.slots.map { s =>
          s.content match {
            case f: FieldSlotContent => f.field.name -> r.getCypherValue(f.key, records.header)
            case x => x.key.withoutType -> r.getCypherValue(x.key, records.header)
          }
        }.toMap
        CypherMap(properties)
      }
      Bag(rows: _*)
    }
  }
}
