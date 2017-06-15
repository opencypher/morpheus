package org.opencypher.spark.impl.instances

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.opencypher.spark.api.expr.{Expr, HasLabel, Property, Var}
import org.opencypher.spark.api.ir.QueryModel
import org.opencypher.spark.api.ir.global.GlobalsRegistry
import org.opencypher.spark.api.record.{FieldSlotContent, OpaqueField, ProjectedExpr, RecordHeader}
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.spark.{SparkCypherGraph, SparkCypherRecords, SparkGraphSpace}
import org.opencypher.spark.api.types.{CTBoolean, CTNode, CTString, _}
import org.opencypher.spark.api.value.{CypherString, CypherValue}
import org.opencypher.spark.impl.instances.spark.cypher._
import org.opencypher.spark.impl.physical.RuntimeContext
import org.opencypher.spark.impl.spark.SparkColumnName
import org.opencypher.spark.impl.syntax.cypher._
import org.opencypher.spark.impl.syntax.header._
import org.opencypher.spark.{TestSession, TestSuiteImpl}
import org.s1ck.gdl.GDLHandler
import org.s1ck.gdl.model.Vertex
import org.scalatest.Assertion

import scala.collection.JavaConverters._

class ExpressionAcceptanceTest extends TestSuiteImpl with TestSession.Fixture {
  val DEFAULT_LABEL = "DEFAULT"

  test("property expression") {
    val theGraph = """(:Person {name: "Mats"})-->(:Person {name: "Martin"})"""

    val graph = theGraph.toGraph

    val result = graph.cypher("MATCH (p:Person) RETURN p.name")

    result.records.toMaps should equal(Set(
      Map("p.name" -> CypherString("Mats")),
      Map("p.name" -> CypherString("Martin"))
    ))
    result.graph shouldMatch theGraph
  }

  implicit class GraphMatcher(graph: SparkCypherGraph) {
    def shouldMatch(gdl: String): Assertion = {
      val expectedGraph = new GDLHandler.Builder()
        .setDefaultEdgeLabel(DEFAULT_LABEL)
        .setDefaultVertexLabel(DEFAULT_LABEL)
        .buildFromString(gdl)

      val expectedNodeIds = expectedGraph.getVertices.asScala.map(_.getId).toSet
//      val expectedRelIds = expectedGraph.getEdges.asScala.map(_.getId)

      val actualNodeIds = graph.nodes(Var("n")(CTNode)).data.select("n").collect().map(_.getLong(0)).toSet
      //val rels = graph.relationships(Var("r")(CTRelationship)).toMaps

      expectedNodeIds should equal(actualNodeIds)
    }

  }

  val _session = session

  implicit class RichString(pattern: String) {
    private val queryGraph = new GDLHandler.Builder()
      .setDefaultEdgeLabel(DEFAULT_LABEL)
      .setDefaultVertexLabel(DEFAULT_LABEL)
      .buildFromString(pattern)

    def toGraph: SparkCypherGraph = new SparkCypherGraph {
      self =>

      override val schema: Schema = {
        val labelAndProps = queryGraph.getVertices.asScala.map { v =>
          v.getLabel -> v.getProperties.asScala.map {
            case (name, prop) => name -> typeOf(prop)
          }
        }

        labelAndProps.foldLeft(Schema.empty) {
          case (acc, (label, props)) => acc.withNodeKeys(label)(props.toSeq: _*)
        }
      }

      override val space: SparkGraphSpace = new SparkGraphSpace {
        override val session: SparkSession = _session
        override val globals: GlobalsRegistry = GlobalsRegistry.fromSchema(schema)
        override val base: SparkCypherGraph = {
          self
        }
      }
      override def relationships(v: Var): SparkCypherRecords = ???
      override def nodes(v: Var): SparkCypherRecords = new SparkCypherRecords {

        private val contents = Seq(OpaqueField(v),
          ProjectedExpr(HasLabel(v, space.globals.labelByName("Person"))(CTBoolean)),
          ProjectedExpr(Property(v, space.globals.propertyKeyByName("name"))(CTString)))

        private def computeLabel(v: Vertex, labelToConsider: String): Boolean = v.getLabel == labelToConsider

        override val data: DataFrame = {
          val longs = queryGraph.getVertices.asScala.map(v => Row(v.getId, v.getLabel == "Person", v.getProperties.get("name"))).toList.asJava
          val schema = StructType(Seq(
            StructField(SparkColumnName.of(contents.head), LongType),
            StructField(SparkColumnName.of(contents(1)), BooleanType),
            StructField(SparkColumnName.of(contents(2)), StringType)))
          session.createDataFrame(longs, schema)
        }

        override val header: RecordHeader = RecordHeader.empty.update(addContents(contents))._1
      }

      override def model: QueryModel[Expr] = ???
      override def details: SparkCypherRecords = ???
    }
  }

  import org.opencypher.spark.impl.instances.spark.RowUtils._

  implicit val context = RuntimeContext(Map.empty, GlobalsRegistry.none)

  implicit class RichRecords(records: SparkCypherRecords) {
    def toMaps: Set[Map[String, CypherValue]] = {
      records.toDF().collect().map { r =>
        records.header.slots.map { s =>
          s.content match {
            case f: FieldSlotContent => f.field.name -> r.getCypherValue(f.key, records.header)
            case x => x.key.withoutType -> r.getCypherValue(x.key, records.header)
          }
        }.toMap
      }.toSet
    }
  }
}
