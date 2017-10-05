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
package org.opencypher.caps.impl.spark.io.neo4j

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.neo4j.driver.internal.{InternalNode, InternalRelationship}
import org.opencypher.caps.api.expr._
import org.opencypher.caps.ir.api.global.{GlobalsRegistry, PropertyKey}
import org.opencypher.caps.api.record.{OpaqueField, ProjectedExpr, RecordHeader, SlotContent}
import org.opencypher.caps.api.schema.{Schema, VerifiedSchema}
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSRecords, CAPSSession}
import org.opencypher.caps.api.types._
import org.opencypher.caps.api.value.CypherValue
import org.opencypher.caps.impl.convert.fromJavaType
import org.opencypher.caps.impl.record.CAPSRecordsTokens
import org.opencypher.caps.impl.spark.SparkColumnName
import org.opencypher.caps.impl.spark.convert.toSparkType
import org.opencypher.caps.impl.spark.exception.Raise
import org.opencypher.caps.impl.syntax.header._

object Neo4jGraphLoader {

  def loadSchema(config: EncryptedNeo4jConfig, nodeQ: String, relQ: String)(implicit caps: CAPSSession): VerifiedSchema = {
    val (nodes, rels) = loadRDDs(config, nodeQ, relQ)

    loadSchema(nodes, rels)
  }

  private def loadSchema(nodes: RDD[InternalNode], rels: RDD[InternalRelationship]): VerifiedSchema = {
    import scala.collection.JavaConverters._

    val nodeSchema = nodes.aggregate(Schema.empty)({
      // TODO: what about nodes without labels?
      case (acc, next) => next.labels().asScala.foldLeft(acc) {
        case (acc2, l) =>
          // for nodes without properties
          val withLabel = acc2.withNodePropertyKeys(l)()
          next.asMap().asScala.foldLeft(withLabel) {
            case (acc3, (k, v)) =>
              acc3.withNodePropertyKeys(l)(k -> fromJavaType(v))
          }
      }
    }, _ ++ _)

    val completeSchema = rels.aggregate(nodeSchema)({
      case (acc, next) =>
        // for rels without properties
        val withType = acc.withRelationshipPropertyKeys(next.`type`())()
        next.asMap().asScala.foldLeft(withType) {
          case (acc3, (k, v)) =>
            acc3.withRelationshipPropertyKeys(next.`type`())(k -> fromJavaType(v))
        }
    },  _ ++ _)

    completeSchema.verify
  }

  def fromNeo4j(config: EncryptedNeo4jConfig)
               (implicit caps: CAPSSession): CAPSGraph =
    fromNeo4j(config, "MATCH (n) RETURN n", "MATCH ()-[r]->() RETURN r")

  def fromNeo4j(config: EncryptedNeo4jConfig, nodeQuery: String, relQuery: String)
               (implicit caps: CAPSSession): CAPSGraph =
    fromNeo4j(config, nodeQuery, relQuery, "source", "rel", "target", None)

  def fromNeo4j(config: EncryptedNeo4jConfig, nodeQuery: String, relQuery: String, schema: VerifiedSchema)
               (implicit caps: CAPSSession): CAPSGraph =
    fromNeo4j(config, nodeQuery, relQuery, "source", "rel", "target", Some(schema))


  def fromNeo4j(config: EncryptedNeo4jConfig, nodeQuery: String, relQuery: String,
                sourceNode: String, rel: String, targetNode: String,
                maybeSchema: Option[VerifiedSchema] = None)
               (implicit caps: CAPSSession): CAPSGraph = {
    val (nodes, rels) = loadRDDs(config, nodeQuery, relQuery)

    val verified = maybeSchema.getOrElse(loadSchema(nodes, rels))
    val context = LoadingContext(verified, GlobalsRegistry.fromSchema(verified))

    createGraph(nodes.cache(), rels.cache(), sourceNode, rel, targetNode)(caps, context)
  }

  private def loadRDDs(config: EncryptedNeo4jConfig, nodeQ: String, relQ: String)(implicit caps: CAPSSession) = {
    val sparkSession = caps.sparkSession
    val neo4j = EncryptedNeo4j(config, sparkSession)
    val nodes = neo4j.cypher(nodeQ).loadNodeRdds.map(row => row(0).asInstanceOf[InternalNode])
    val rels = neo4j.cypher(relQ).loadRowRdd.map(row => row(0).asInstanceOf[InternalRelationship])

    nodes -> rels
  }

  case class LoadingContext(verifiedSchema: VerifiedSchema, globals: GlobalsRegistry) {
    def schema = verifiedSchema.schema
  }


  // TODO: Rethink caching. Currently the RDDs are cached before calling this method.

  private def createGraph(inputNodes: RDD[InternalNode], inputRels: RDD[InternalRelationship],
                          sourceNode: String = "source", rel: String = "rel", targetNode: String = "target")
                         (implicit caps: CAPSSession, context: LoadingContext): CAPSGraph =
    new CAPSGraph {

      override val schema: Schema = context.schema

      override def session: CAPSSession = caps

      override protected def graph: CAPSGraph = this

      private def sparkSession = session.sparkSession

      override def nodes(name: String, cypherType: CTNode): CAPSRecords = {
        val fields = computeNodeFields(name, cypherType)
        computeRecords(name, cypherType, fields) { (header, struct) =>
          inputNodes.filter(filterNode(cypherType)).map(nodeToRow(header, struct))
        }
      }

      override def relationships(name: String, cypherType: CTRelationship): CAPSRecords = {
        val fields = computeRelFields(name, cypherType)
        computeRecords(name, cypherType, fields) { (header, struct) =>
          inputRels.filter(filterRel(cypherType)).map(relToRow(header, struct))
        }
      }

      override def union(other: CAPSGraph): CAPSGraph =
        Raise.unsupportedArgument("union with neo graph")

      private def computeRecords(name: String, cypherType: CypherType, fields: Seq[(SlotContent, StructField)])
                                (computeRdd: (RecordHeader, StructType) => RDD[Row]): CAPSRecords = {
        val header = computeHeader(fields)
        val struct = StructType(fields.map(_._2).toArray)
        val rdd = computeRdd(header, struct)
        val slot = header.slotFor(Var(name)(cypherType))
        val rawData = session.sparkSession.createDataFrame(rdd, struct)
        val col = rawData.col(rawData.columns(slot.index))
        val recordData = rawData.repartition(col).sortWithinPartitions(col)
        CAPSRecords.create(header, recordData)
      }

      private def computeHeader(fields: Seq[(SlotContent, StructField)]) =
        fields.map(_._1).foldLeft(RecordHeader.empty) {
          case (acc, next) => acc.update(addContent(next))._1
        }

      private def computeNodeFields(name: String, cypherType: CTNode)
                                   (implicit context: LoadingContext): Seq[(SlotContent, StructField)] = {
        val node = Var(name)(cypherType)

        val schema = context.schema
        val tokens = context.globals.tokens

        val labels = if (cypherType.labels.isEmpty) schema.labels else cypherType.labels

        val labelFields = labels.map { name =>
          val label = HasLabel(node, tokens.labelByName(name))(CTBoolean)
          val slot = ProjectedExpr(label)
          val field = StructField(SparkColumnName.of(slot), BooleanType, nullable = false)
          slot -> field
        }
        val propertyFields = labels.flatMap { l =>
          schema.nodeKeys(l).map {
            case (key, t) =>
              val property = Property(node, tokens.propertyKeyByName(key))(t)
              val slot = ProjectedExpr(property)
              val field = StructField(SparkColumnName.of(slot), toSparkType(t), nullable = true)
              slot -> field
          }
        }
        val nodeSlot = OpaqueField(node)
        val nodeField = StructField(SparkColumnName.of(nodeSlot), LongType, nullable = false)
        val slotField = nodeSlot -> nodeField
        Seq(slotField) ++ labelFields ++ propertyFields
      }

      private def computeRelFields(name: String, cypherType: CTRelationship)
                                  (implicit context: LoadingContext): Seq[(SlotContent, StructField)] = {
        val rel = Var(name)(cypherType)

        val schema = context.schema
        val tokens = context.globals.tokens

        val propertyFields = schema.relationshipTypes.flatMap { typ =>
          schema.relationshipKeys(typ).map {
            case (key, t) =>
              val property = Property(rel, tokens.propertyKeyByName(key))(t)
              val slot = ProjectedExpr(property)
              val field = StructField(SparkColumnName.of(slot), toSparkType(t), nullable = true)
              slot -> field
          }
        }
        val typeSlot = ProjectedExpr(OfType(rel)(CTString))
        val typeField = StructField(SparkColumnName.of(typeSlot), StringType, nullable = false)

        val idSlot = OpaqueField(rel)
        val idField = StructField(SparkColumnName.of(idSlot), LongType, nullable = false)

        val sourceSlot = ProjectedExpr(StartNode(rel)(CTNode))
        val sourceField = StructField(SparkColumnName.of(sourceSlot), LongType, nullable = false)
        val targetSlot = ProjectedExpr(EndNode(rel)(CTNode))
        val targetField = StructField(SparkColumnName.of(targetSlot), LongType, nullable = false)

        Seq(sourceSlot -> sourceField, idSlot -> idField,
          typeSlot -> typeField, targetSlot -> targetField) ++ propertyFields
      }
    }

  private case class filterNode(nodeDef: CTNode)
                               (implicit context: LoadingContext) extends (InternalNode => Boolean) {

    val requiredLabels: Set[String] = nodeDef.labels

    override def apply(importedNode: InternalNode): Boolean = requiredLabels.forall(importedNode.hasLabel)
  }

  private case class nodeToRow(header: RecordHeader, schema: StructType)
                              (implicit context: LoadingContext) extends (InternalNode => Row) {
    override def apply(importedNode: InternalNode): Row = {
      val graphSchema = context.schema
      val globals = context.globals

      import scala.collection.JavaConverters._

      val props = importedNode.asMap().asScala
      val labels = importedNode.labels().asScala.toSet

      val schemaKeyTypes = labels.map { label => graphSchema.nodeKeys(label) }.reduce(_ ++ _)

      val values = header.slots.map { slot =>
        slot.content.key match {
          case Property(_, PropertyKey(keyName)) =>
            val propValue = schemaKeyTypes.get(keyName) match {
              case Some(t) if t == slot.content.cypherType => props.get(keyName).orNull
              case _ => null
            }
            importedToSparkEncodedCypherValue(schema(slot.index).dataType, propValue)
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

  private case class filterRel(relDef: CTRelationship)
                              (implicit context: LoadingContext) extends (InternalRelationship => Boolean) {

    override def apply(importedRel: InternalRelationship): Boolean = relDef.types.forall(importedRel.hasType)
  }

  private case class relToRow(header: RecordHeader, schema: StructType)
                             (implicit context: LoadingContext) extends (InternalRelationship => Row) {
    override def apply(importedRel: InternalRelationship): Row = {
      val graphSchema = context.schema
      val tokens = context.globals.tokens

      import scala.collection.JavaConverters._

      val relType = importedRel.`type`()
      val props = importedRel.asMap().asScala

      val schemaKeyTypes = graphSchema.relationshipKeys(relType)

      val values = header.slots.map { slot =>
        slot.content.key match {
          case Property(_, PropertyKey(keyName)) =>
            val propValue = schemaKeyTypes.get(keyName) match {
              case Some(t) if t == slot.content.cypherType => props.get(keyName).orNull
              case _ => null
            }
            importedToSparkEncodedCypherValue(schema(slot.index).dataType, propValue)

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
    case BinaryType => if (value == null) null else value.toString.getBytes // TODO: Call kryo
    case _ => CypherValue(value)
  }
}
