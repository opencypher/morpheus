/*
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
package org.opencypher.caps.test.support

import java.net.URI

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.opencypher.caps.api.expr.{HasLabel, Property, Var}
import org.opencypher.caps.api.io.PersistMode
import org.opencypher.caps.api.record.RecordHeader
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSRecords, CAPSResult, CAPSSession}
import org.opencypher.caps.api.types.{CTNode, CTRelationship}
import org.opencypher.caps.api.util.parsePathOrURI
import org.opencypher.caps.api.value.CypherValue
import org.opencypher.caps.impl.convert.fromJavaType
import org.opencypher.caps.impl.spark.convert.toSparkType
import org.opencypher.caps.impl.spark.io.CAPSGraphSourceImpl
import org.opencypher.caps.test.BaseTestSuite
import org.opencypher.caps.test.fixture.{CAPSSessionFixture, SparkSessionFixture}
import org.s1ck.gdl.GDLHandler
import org.s1ck.gdl.model.Element
import org.scalatest.Assertion

import scala.collection.JavaConverters._
import scala.collection.immutable.{Map, Seq}

trait GraphMatchingTestSupport {

  self: BaseTestSuite with SparkSessionFixture with CAPSSessionFixture =>

  val DEFAULT_LABEL              = "DEFAULT"
  val sparkSession: SparkSession = session

  implicit class GraphsMatcher(graphs: Map[String, CAPSGraph]) {
    def shouldMatch(expectedGraphs: CAPSGraph*): Unit = {
      withClue("expected and actual must have same size") {
        graphs.size should equal(expectedGraphs.size)
      }

      graphs.values.zip(expectedGraphs).foreach {
        case (actual, expected) => verify(actual, expected)
      }
    }

    private def verify(actual: CAPSGraph, expected: CAPSGraph): Assertion = {
      val expectedNodeIds = expected.nodes("n").data.select("n").collect().map(_.getLong(0)).toSet
      val expectedRelIds =
        expected.relationships("r").data.select("r").collect().map(_.getLong(0)).toSet
      val actualNodeIds = actual.nodes("n").data.select("n").collect().map(_.getLong(0)).toSet
      val actualRelIds =
        actual.relationships("r").data.select("r").collect().map(_.getLong(0)).toSet

      expectedNodeIds should equal(actualNodeIds)
      expectedRelIds should equal(actualRelIds)
    }
  }

  case class TestGraph(gdl: String)(implicit caps: CAPSSession) {

    private val queryGraph = new GDLHandler.Builder()
      .setDefaultEdgeLabel(DEFAULT_LABEL)
      .setDefaultVertexLabel(DEFAULT_LABEL)
      .buildFromString(gdl)

    def mountAt(pathOrUri: String): Unit =
      mountAt(parsePathOrURI(pathOrUri))

    def mountAt(uri: URI): Unit =
      caps.mountSourceAt(TestGraphSource(uri, this), uri)

    def cypher(query: String): CAPSResult =
      caps.cypher(graph, query, Map.empty)

    def cypher(query: String, parameters: Map[String, CypherValue]): CAPSResult =
      caps.cypher(graph, query, parameters)

    // TODO: Use lazy caps graph
    lazy val graph: CAPSGraph = new CAPSGraph {
      self =>

      override def session: CAPSSession       = caps
      override protected def graph: CAPSGraph = this

      override def cache() = this

      override def persist() = this

      override def persist(storageLevel: StorageLevel) = this

      override def unpersist() = this

      override def unpersist(blocking: Boolean) = this

      private def extractFromElement(e: Element) = e.getLabels.asScala.map { label =>
        label -> e.getProperties.asScala.map {
          case (name, prop) => name -> fromJavaType(prop)
        }
      }

      override val schema: Schema = {
        val labelAndProps = queryGraph.getVertices.asScala.flatMap(extractFromElement)
        val typesAndProps = queryGraph.getEdges.asScala.flatMap(extractFromElement)
        val vertexLabelCombinations = queryGraph.getVertices.asScala.map { vertex =>
          vertex.getLabels.asScala
        }.toSet

        val schemaWithLabels = labelAndProps.foldLeft(Schema.empty) {
          case (acc, (label, props)) => acc.withNodePropertyKeys(label)(props.toSeq: _*)
        }

        val schemaWithLabelCombinations = vertexLabelCombinations.foldLeft(schemaWithLabels) {
          (acc, labels) =>
            if (labels.size > 1)
              acc.withLabelCombination(labels: _*)
            else
              acc
        }

        typesAndProps.foldLeft(schemaWithLabelCombinations) {
          case (acc, (t, props)) => acc.withRelationshipPropertyKeys(t)(props.toSeq: _*)
        }
      }

      override def nodes(name: String, cypherType: CTNode): CAPSRecords = {
        val header = RecordHeader.nodeFromSchema(Var(name)(cypherType), schema, cypherType.labels)

        val data = {
          val nodes = queryGraph.getVertices.asScala
            .filter(v => v.getLabels.containsAll(cypherType.labels.asJava))
            .map { v =>
              val exprs = header.slots.map(_.content.key)
              val labelFields = exprs.collect {
                case HasLabel(_, label) => v.getLabels.contains(label.name)
              }
              val propertyFields = exprs.collect {
                case p @ Property(_, k) =>
                  val pValue = v.getProperties.get(k.name)
                  if (fromJavaType(pValue) == p.cypherType) pValue
                  else null
              }

              Row(v.getId +: (labelFields ++ propertyFields): _*)
            }
            .toList
            .asJava

          val fields = header.slots.map { s =>
            StructField(context.columnName(s), toSparkType(s.content.cypherType))
          }

          sparkSession.createDataFrame(nodes, StructType(fields))
        }
        CAPSRecords.create(header, data)
      }

      override def union(other: CAPSGraph): CAPSGraph = ???

      override def relationships(name: String, cypherType: CTRelationship): CAPSRecords = {

        val header = RecordHeader.relationshipFromSchema(Var(name)(cypherType), schema)

        val data = {
          val rels = queryGraph.getEdges.asScala
            .filter(e =>
              cypherType.types.asJava.isEmpty || cypherType.types.asJava.containsAll(e.getLabels))
            .map { e =>
              val staticFields = Seq(e.getSourceVertexId, e.getId, e.getLabel, e.getTargetVertexId)

              val propertyFields = header.slots.slice(4, header.slots.size).map(_.content.key).map {
                case Property(_, k) => e.getProperties.get(k.name)
                case _ =>
                  throw new IllegalArgumentException("Only properties expected in the header")
              }

              Row(staticFields ++ propertyFields: _*)
            }
            .toList
            .asJava

          val fields = header.slots.map { s =>
            StructField(context.columnName(s), toSparkType(s.content.cypherType))
          }

          sparkSession.createDataFrame(rels, StructType(fields))
        }
        CAPSRecords.create(header, data)
      }
    }
  }

  private case class TestGraphSource(canonicalURI: URI, testGraph: TestGraph)
      extends CAPSGraphSourceImpl {

    override def sourceForGraphAt(uri: URI): Boolean                   = uri == canonicalURI
    override def create: CAPSGraph                                     = ???
    override def graph: CAPSGraph                                      = testGraph.graph
    override def schema: Option[Schema]                                = None
    override def store(graph: CAPSGraph, mode: PersistMode): CAPSGraph = ???
    override def delete(): Unit                                        = ???
  }
}
