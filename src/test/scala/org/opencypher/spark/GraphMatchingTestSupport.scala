package org.opencypher.spark

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.opencypher.spark.api.classes.Cypher
import org.opencypher.spark.api.expr.{Expr, HasLabel, Property, Var}
import org.opencypher.spark.api.ir.QueryModel
import org.opencypher.spark.api.ir.global.TokenRegistry
import org.opencypher.spark.api.record.{FieldSlotContent, RecordHeader}
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.spark.{SparkCypherGraph, SparkCypherRecords, SparkCypherResult, SparkGraphSpace}
import org.opencypher.spark.api.types.{CTNode, CTRelationship}
import org.opencypher.spark.api.value.{CypherMap, CypherValue}
import org.opencypher.spark.impl.convert.{fromJavaType, toSparkType}
import org.opencypher.spark.impl.physical.RuntimeContext
import org.opencypher.spark.impl.record.SparkCypherRecordsTokens
import org.s1ck.gdl.GDLHandler
import org.s1ck.gdl.model.Element
import org.scalatest.Assertion

import scala.collection.JavaConverters._

trait GraphMatchingTestSupport extends TestSession.Fixture {
  self: TestSuiteImpl =>

  val DEFAULT_LABEL = "DEFAULT"
  val sparkSession = session

  implicit val context: RuntimeContext = RuntimeContext.empty

  implicit class GraphMatcher(graph: SparkCypherGraph) {
    def shouldMatch(expectedGraph: SparkCypherGraph): Assertion = {
      val expectedNodeIds = expectedGraph.nodes(Var("n")(CTNode)).data.select("n").collect().map(_.getLong(0)).toSet
      val expectedRelIds = expectedGraph.relationships(Var("r")(CTRelationship)).data.select("r").collect().map(_.getLong(0)).toSet

      val actualNodeIds = graph.nodes(Var("n")(CTNode)).data.select("n").collect().map(_.getLong(0)).toSet
      val actualRelIds = graph.relationships(Var("r")(CTRelationship)).data.select("r").collect().map(_.getLong(0)).toSet

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

      private def extractFromElement(e: Element) = e.getLabel -> e.getProperties.asScala.map {
        case (name, prop) => name -> fromJavaType(prop)
      }

      override val schema: Schema = {
        val labelAndProps = queryGraph.getVertices.asScala.map(extractFromElement)
        val typesAndProps = queryGraph.getEdges.asScala.map(extractFromElement)

        val schemaWithLabels = labelAndProps.foldLeft(Schema.empty) {
          case (acc, (label, props)) => acc.withNodeKeys(label)(props.toSeq: _*)
        }

        typesAndProps.foldLeft(schemaWithLabels) {
          case (acc, (t, props)) => acc.withRelationshipKeys(t)(props.toSeq: _*)
        }
      }

      override val space: SparkGraphSpace = new SparkGraphSpace {
        override val session: SparkSession = sparkSession
        override val tokens: SparkCypherRecordsTokens = SparkCypherRecordsTokens(TokenRegistry.fromSchema(schema))
        override val base: SparkCypherGraph = {
          self
        }
      }

      override def nodes(v: Var): SparkCypherRecords = {

        val header = RecordHeader.nodeFromSchema(v, schema, space.tokens.registry)

        val data = {
          val nodes = queryGraph.getVertices.asScala.map { v =>
            val exprs = header.slots.map(_.content.key)
            val labelFields: Seq[Boolean] = exprs.collect {
              case HasLabel(_, label) => v.getLabel == label.name
            }
            val propertyFields: Seq[Any] = exprs.collect {
              case Property(_, k) => v.getProperties.get(k.name)
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

      override def relationships(v: Var): SparkCypherRecords = {

        val header = RecordHeader.relationshipFromSchema(v, schema, space.tokens.registry)

        val data = {
          val rels = queryGraph.getEdges.asScala.map { e =>
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

      override def details: SparkCypherRecords = ???
    }
  }

  implicit class RichRecords(records: SparkCypherRecords) {
    import org.opencypher.spark.impl.instances.spark.RowUtils._

    def toMaps: Set[CypherMap] = {
      records.toDF().collect().map { r =>
        val properties = records.header.slots.map { s =>
          s.content match {
            case f: FieldSlotContent => f.field.name -> r.getCypherValue(f.key, records.header)
            case x => x.key.withoutType -> r.getCypherValue(x.key, records.header)
          }
        }.toMap
        CypherMap(properties)
      }.toSet
    }
  }
}
