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
package org.opencypher.caps.impl.spark.io.neo4j

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.neo4j.driver.internal.{InternalNode, InternalRelationship}
import org.opencypher.caps.api.expr._
import org.opencypher.caps.api.record.{OpaqueField, ProjectedExpr, RecordHeader, SlotContent}
import org.opencypher.caps.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.caps.api.schema.Schema.NoLabel
import org.opencypher.caps.api.schema.{PropertyKeys, Schema, VerifiedSchema}
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSRecords, CAPSSession}
import org.opencypher.caps.api.types._
import org.opencypher.caps.api.value.CypherValue
import org.opencypher.caps.impl.convert.fromJavaType
import org.opencypher.caps.impl.record.CAPSRecordHeader
import org.opencypher.caps.impl.spark.SparkColumnName
import org.opencypher.caps.impl.exception.Raise
import org.opencypher.caps.impl.spark.io.neo4j.external.{Neo4j, Neo4jConfig}
import org.opencypher.caps.impl.syntax.RecordHeaderSyntax._
import org.opencypher.caps.ir.api.PropertyKey

import scala.collection.JavaConverters._

object Neo4jGraphLoader {

  def loadSchema(config: Neo4jConfig, nodeQ: String, relQ: String)(implicit caps: CAPSSession): VerifiedSchema = {
    val (nodes, rels) = loadRDDs(config, nodeQ, relQ)

    loadSchema(nodes, rels)
  }

  private def loadSchema(nodes: RDD[InternalNode], rels: RDD[InternalRelationship]): VerifiedSchema = {

    val nodeSchema = nodes.aggregate(Schema.empty)({
      case (schema, node) =>
        val labels =
          if (node.labels().isEmpty)  // could remove this unless we rely on `NoLabel`
            NoLabel
          else
            node.labels().asScala.toSet

        schema.withNodePropertyKeys(labels, convertProperties(node.asMap()))
    }, _ ++ _)

    val completeSchema = rels.aggregate(nodeSchema)({
      case (schema, next) =>
        schema.withRelationshipPropertyKeys(next.`type`(), convertProperties(next.asMap()))
    },  _ ++ _)

    completeSchema.verify
  }

  private def convertProperties(properties: java.util.Map[String, AnyRef]): PropertyKeys = {
    if (properties.isEmpty) {
      PropertyKeys.empty
    } else {
      properties.asScala.map {
        case (k, v) => k -> fromJavaType(v)
      }.toMap
    }
  }

  def fromNeo4j(config: Neo4jConfig)
               (implicit caps: CAPSSession): CAPSGraph =
    fromNeo4j(config, "MATCH (n) RETURN n", "MATCH ()-[r]->() RETURN r")

  def fromNeo4j(config: Neo4jConfig, nodeQuery: String, relQuery: String)
               (implicit caps: CAPSSession): CAPSGraph =
    fromNeo4j(config, nodeQuery, relQuery, "source", "rel", "target", None)

  def fromNeo4j(config: Neo4jConfig, nodeQuery: String, relQuery: String, schema: VerifiedSchema)
               (implicit caps: CAPSSession): CAPSGraph =
    fromNeo4j(config, nodeQuery, relQuery, "source", "rel", "target", Some(schema))


  def fromNeo4j(config: Neo4jConfig, nodeQuery: String, relQuery: String,
                sourceNode: String, rel: String, targetNode: String,
                maybeSchema: Option[VerifiedSchema] = None)
               (implicit caps: CAPSSession): CAPSGraph = {
    val (nodes, rels) = loadRDDs(config, nodeQuery, relQuery)

    val verified = maybeSchema.getOrElse(loadSchema(nodes, rels))
    val context = LoadingContext(verified)

    createGraph(nodes, rels, sourceNode, rel, targetNode)(caps, context)
  }

  private def loadRDDs(config: Neo4jConfig, nodeQ: String, relQ: String)(implicit caps: CAPSSession) = {
    val sparkSession = caps.sparkSession
    val neo4j = Neo4j(config, sparkSession)
    val nodes = neo4j.cypher(nodeQ).loadNodeRdds.map(row => row(0).asInstanceOf[InternalNode])
    val rels = neo4j.cypher(relQ).loadRowRdd.map(row => row(0).asInstanceOf[InternalRelationship])

    nodes -> rels
  }

  case class LoadingContext(verifiedSchema: VerifiedSchema) {
    def schema: Schema = verifiedSchema.schema
  }

  private def createGraph(inputNodes: RDD[InternalNode], inputRels: RDD[InternalRelationship],
                          sourceNode: String = "source", rel: String = "rel", targetNode: String = "target")
                         (implicit caps: CAPSSession, context: LoadingContext): CAPSGraph =
    new CAPSGraph {

      override val schema: Schema = context.schema

      override def cache(): CAPSGraph = map(_.cache(), _.cache())

      override def persist(): CAPSGraph = map(_.persist(), _.persist())

      override def persist(storageLevel: StorageLevel): CAPSGraph = map(_.persist(storageLevel), _.persist(storageLevel))

      override def unpersist(): CAPSGraph = map(_.unpersist(), _.unpersist())

      override def unpersist(blocking: Boolean): CAPSGraph = map(_.unpersist(blocking), _.unpersist(blocking))

      private def map(f: RDD[InternalNode] => RDD[InternalNode], g: RDD[InternalRelationship] => RDD[InternalRelationship]) =
        // We need to construct new RDDs since otherwise providing a different storage level may fail
        createGraph(f(inputNodes.filter(_ => true)), g(inputRels.filter(_ => true)), sourceNode, rel, targetNode)

      override def session: CAPSSession = caps

      override protected def graph: CAPSGraph = this

      override def nodes(name: String, cypherType: CTNode): CAPSRecords = {
        val header = RecordHeader.nodeFromSchema(Var(name)(cypherType), schema)

        computeRecords(name, cypherType, header) { (header, struct) =>
          inputNodes.filter(filterNode(cypherType)).map(nodeToRow(header, struct))
        }
      }

      override def relationships(name: String, cypherType: CTRelationship): CAPSRecords = {
        val header = RecordHeader.relationshipFromSchema(Var(name)(cypherType), schema)

        computeRecords(name, cypherType, header) { (header, struct) =>
          inputRels.filter(filterRel(cypherType)).map(relToRow(header, struct))
        }
      }

      override def union(other: CAPSGraph): CAPSGraph =
        Raise.unsupportedArgument("union with neo graph")

      private def computeRecords(name: String, cypherType: CypherType, header: RecordHeader)
                                (computeRdd: (RecordHeader, StructType) => RDD[Row]): CAPSRecords = {
        val struct = CAPSRecordHeader.asSparkStructType(header)
        val rdd = computeRdd(header, struct)
        val slot = header.slotFor(Var(name)(cypherType))
        val rawData = session.sparkSession.createDataFrame(rdd, struct)
        val col = rawData.col(rawData.columns(slot.index))
        val recordData = rawData.repartition(col).sortWithinPartitions(col)
        CAPSRecords.create(header, recordData)
      }

      override def toString = "Neo4jGraph"
    }

  private case class filterNode(nodeDef: CTNode)
                               (implicit context: LoadingContext) extends (InternalNode => Boolean) {

    val requiredLabels: Set[String] = nodeDef.labels

    override def apply(importedNode: InternalNode): Boolean = requiredLabels.forall(importedNode.hasLabel)
  }

  private case class nodeToRow(header: RecordHeader, schema: StructType)
                              (implicit context: LoadingContext) extends (InternalNode => Row) {
    override def apply(importedNode: InternalNode): Row = {
      import scala.collection.JavaConverters._

      val props = importedNode.asMap().asScala
      val labels = importedNode.labels().asScala.toSet

      val values = header.slots.map { slot =>
        slot.content.key match {
          case Property(_, PropertyKey(keyName)) =>
            val propValue = props.get(keyName).orNull
            val dataType = schema(SparkColumnName.of(slot)).dataType
            importedToSparkEncodedCypherValue(dataType, propValue)
          case HasLabel(_, label) =>
            labels(label.name)
          case _: Var =>
            importedNode.id()

          case _ =>
            Raise.impossible("Nothing else should appear")
        }
      }

      Row(values: _*)
    }
  }

  private case class filterRel(relType: CTRelationship)
                              (implicit context: LoadingContext) extends (InternalRelationship => Boolean) {

    override def apply(importedRel: InternalRelationship): Boolean =
      relType.types.isEmpty || relType.types.exists(importedRel.hasType)
  }

  private case class relToRow(header: RecordHeader, schema: StructType)
                             (implicit context: LoadingContext) extends (InternalRelationship => Row) {
    override def apply(importedRel: InternalRelationship): Row = {
      import scala.collection.JavaConverters._

      val relType = importedRel.`type`()
      val props = importedRel.asMap().asScala

      val values = header.slots.map { slot =>
        slot.content.key match {
          case Property(_, PropertyKey(keyName)) =>
            val propValue = props.get(keyName).orNull
            val dataType = schema(SparkColumnName.of(slot)).dataType
            importedToSparkEncodedCypherValue(dataType, propValue)

          case _: StartNode =>
            importedRel.startNodeId()

          case _: EndNode =>
            importedRel.endNodeId()

          case _: OfType =>
            relType

          case _: Var =>
            importedRel.id()

          case x =>
            Raise.invalidArgument("One of: property lookup, startNode(), endNode(), id(), variable", x.toString)
        }
      }

      Row(values: _*)
    }
  }

  private def importedToSparkEncodedCypherValue(typ: DataType, value: AnyRef): AnyRef = typ match {
    case StringType | LongType | BooleanType | DoubleType => value
    case _ => CypherValue(value)
  }
}
