package org.opencypher.spark

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.opencypher.spark.api.expr.{Expr, HasLabel, Property, Var}
import org.opencypher.spark.api.ir.QueryModel
import org.opencypher.spark.api.ir.global.TokenRegistry
import org.opencypher.spark.api.record.{FieldSlotContent, RecordHeader}
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.spark.{SparkCypherGraph, SparkCypherRecords, SparkGraphSpace}
import org.opencypher.spark.api.types.{CTNode, CTRelationship, typeOf}
import org.opencypher.spark.api.value.CypherValue
import org.opencypher.spark.impl.physical.RuntimeContext
import org.opencypher.spark.impl.record.SparkCypherRecordsTokens
import org.opencypher.spark.impl.spark.toSparkType
import org.s1ck.gdl.GDLHandler
import org.s1ck.gdl.model.Element
import org.scalatest.Assertion

import scala.collection.JavaConverters._

trait GraphMatchingTestSupport {
  self: TestSuiteImpl =>

  val DEFAULT_LABEL = "DEFAULT"

  implicit val context: RuntimeContext = RuntimeContext.empty

  implicit class GraphMatcher(graph: SparkCypherGraph) {
    def shouldMatch(gdl: String): Assertion = {
      val expectedGraph = new GDLHandler.Builder()
        .setDefaultEdgeLabel(DEFAULT_LABEL)
        .setDefaultVertexLabel(DEFAULT_LABEL)
        .buildFromString(gdl)

      val expectedNodeIds = expectedGraph.getVertices.asScala.map(_.getId).toSet
      val expectedRelIds = expectedGraph.getEdges.asScala.map(_.getId).toSet

      val actualNodeIds = graph.nodes(Var("n")(CTNode)).data.select("n").collect().map(_.getLong(0)).toSet
      val actualRelIds = graph.relationships(Var("r")(CTRelationship)).data.select("r").collect().map(_.getLong(0)).toSet

      expectedNodeIds should equal(actualNodeIds)
      expectedRelIds should equal(actualRelIds)
    }
  }

  implicit class RichString(pattern: String)(implicit _session: SparkSession) {
    private val queryGraph = new GDLHandler.Builder()
      .setDefaultEdgeLabel(DEFAULT_LABEL)
      .setDefaultVertexLabel(DEFAULT_LABEL)
      .buildFromString(pattern)

    def toGraph: SparkCypherGraph = new SparkCypherGraph {
      self =>

      private def extractFromElement(e: Element) = e.getLabel -> e.getProperties.asScala.map {
        case (name, prop) => name -> typeOf(prop)
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
        override val session: SparkSession = _session
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

          val schema = StructType(fields)
          _session.createDataFrame(nodes, schema)
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

          val schema = StructType(fields)

          _session.createDataFrame(rels, schema)
        }
        SparkCypherRecords.create(header, data)(space)
      }

      override def model: QueryModel[Expr] = ???
      override def details: SparkCypherRecords = ???
    }
  }


  implicit class RichRecords(records: SparkCypherRecords) {
    import org.opencypher.spark.impl.instances.spark.RowUtils._

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
