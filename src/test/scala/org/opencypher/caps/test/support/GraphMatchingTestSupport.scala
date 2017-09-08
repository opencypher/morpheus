package org.opencypher.caps.test.support

import java.net.URI

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.opencypher.caps.api.expr.{HasLabel, Property, Var}
import org.opencypher.caps.api.io.{PersistMode, parseURI}
import org.opencypher.caps.api.ir.global.TokenRegistry
import org.opencypher.caps.api.record.{FieldSlotContent, RecordHeader}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSRecords, CAPSResult, CAPSSession}
import org.opencypher.caps.api.types.{CTNode, CTRelationship}
import org.opencypher.caps.api.value.{CypherMap, CypherValue}
import org.opencypher.caps.impl.convert.fromJavaType
import org.opencypher.caps.impl.record.CAPSRecordsTokens
import org.opencypher.caps.impl.spark.convert.toSparkType
import org.opencypher.caps.impl.spark.io.CAPSGraphSourceImpl
import org.opencypher.caps.impl.spark.physical.RuntimeContext
import org.opencypher.caps.test.BaseTestSuite
import org.opencypher.caps.test.fixture.{CAPSSessionFixture, SparkSessionFixture}
import org.s1ck.gdl.GDLHandler
import org.s1ck.gdl.model.Element
import org.scalatest.Assertion

import scala.collection.Bag
import scala.collection.JavaConverters._
import scala.collection.immutable.{HashedBagConfiguration, Map, Seq}

trait GraphMatchingTestSupport {

  self: BaseTestSuite with SparkSessionFixture with CAPSSessionFixture =>

  implicit val bagConfig: HashedBagConfiguration[CypherMap] = Bag.configuration.compact[CypherMap]
  val DEFAULT_LABEL = "DEFAULT"
  val sparkSession: SparkSession = session

  implicit val context: RuntimeContext = RuntimeContext.empty

  implicit class GraphMatcher(graph: CAPSGraph) {
    def shouldMatch(expectedGraph: CAPSGraph): Assertion = {
      val expectedNodeIds = expectedGraph.nodes("n").data.select("n").collect().map(_.getLong(0)).toSet
      val expectedRelIds = expectedGraph.relationships("r").data.select("r").collect().map(_.getLong(0)).toSet

      val actualNodeIds = graph.nodes("n").data.select("n").collect().map(_.getLong(0)).toSet
      val actualRelIds = graph.relationships("r").data.select("r").collect().map(_.getLong(0)).toSet

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
      mountAt(parseURI(pathOrUri))

    def mountAt(uri: URI): Unit =
      caps.mountSourceAt(TestGraphSource(uri, this), uri)

    def cypher(query: String): CAPSResult =
      caps.cypher(graph, query, Map.empty)

    def cypher(query: String, parameters: Map[String, CypherValue]): CAPSResult =
      caps.cypher(graph, query, parameters)

    lazy val graph: CAPSGraph = new CAPSGraph {
      self =>

      override def session: CAPSSession = caps
      override protected def graph: CAPSGraph = this

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

      override val tokens = CAPSRecordsTokens(TokenRegistry.fromSchema(schema))

      override def nodes(name: String, cypherType: CTNode): CAPSRecords = {
        val header = RecordHeader.nodeFromSchema(Var(name)(cypherType), schema, tokens.registry,
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
        CAPSRecords.create(header, data)
      }

      override def relationships(name: String, cypherType: CTRelationship): CAPSRecords = {

        val header = RecordHeader.relationshipFromSchema(Var(name)(cypherType), schema, tokens.registry)

        val data = {
          val rels = queryGraph.getEdges.asScala
            .filter(e => cypherType.types.asJava.isEmpty || cypherType.types.asJava.containsAll(e.getLabels))
            .map { e =>
              val staticFields = Seq(e.getSourceVertexId, e.getId, tokens.relTypeId(e.getLabel).toLong, e.getTargetVertexId)

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
        CAPSRecords.create(header, data)
      }
    }
  }

  // TODO: Move to RecordMatchingTestSupport
  implicit class RichRecords(records: CAPSRecords) {
    import org.opencypher.caps.impl.spark.RowUtils._

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

  private case class TestGraphSource(canonicalURI: URI, testGraph: TestGraph)
    extends CAPSGraphSourceImpl {

    private lazy val capsGraph = testGraph.graph
    override def sourceForGraphAt(uri: URI): Boolean = uri == canonicalURI
    override def create: CAPSGraph = ???
    override def graph: CAPSGraph = capsGraph
    override def schema: Option[Schema] = None
    override def persist(mode: PersistMode, graph: CAPSGraph): CAPSGraph = ???
    override def delete(): Unit = ???
  }
}
