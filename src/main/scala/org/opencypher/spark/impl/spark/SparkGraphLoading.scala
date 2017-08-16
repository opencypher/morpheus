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
package org.opencypher.spark.impl.spark

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.neo4j.driver.internal.{InternalNode, InternalRelationship}
import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir.global.{GlobalsRegistry, PropertyKey, TokenRegistry}
import org.opencypher.spark.api.record.{OpaqueField, ProjectedExpr, RecordHeader, SlotContent}
import org.opencypher.spark.api.schema.{Schema, VerifiedSchema}
import org.opencypher.spark.api.spark.{SparkCypherGraph, SparkCypherRecords, SparkGraphSpace}
import org.opencypher.spark.api.types._
import org.opencypher.spark.api.value.CypherValue
import org.opencypher.spark.impl.convert.{fromJavaType, toSparkType}
import org.opencypher.spark.impl.record.SparkCypherRecordsTokens
import org.opencypher.spark.impl.syntax.header._

trait SparkGraphLoading {

  def loadSchema(nodeQ: String, relQ: String)(implicit sc: SparkSession): VerifiedSchema = {
    val (nodes, rels) = loadRDDs(nodeQ, relQ)

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

  def fromNeo4j(nodeQuery: String, relQuery: String)
               (implicit sc: SparkSession): SparkGraphSpace =
    fromNeo4j(nodeQuery, relQuery, "source", "rel", "target", None)

  def fromNeo4j(nodeQuery: String, relQuery: String, schema: VerifiedSchema)
               (implicit sc: SparkSession): SparkGraphSpace =
    fromNeo4j(nodeQuery, relQuery, "source", "rel", "target", Some(schema))


  def fromNeo4j(nodeQuery: String, relQuery: String,
                sourceNode: String, rel: String, targetNode: String,
                maybeSchema: Option[VerifiedSchema] = None)
               (implicit sc: SparkSession): SparkGraphSpace = {
    val (nodes, rels) = loadRDDs(nodeQuery, relQuery)

    val verified = maybeSchema.getOrElse(loadSchema(nodes, rels))

    implicit val context = LoadingContext(verified.schema, GlobalsRegistry(TokenRegistry.fromSchema(verified)))

    createSpace(nodes, rels, sourceNode, rel, targetNode)
  }

  private def loadRDDs(nodeQ: String, relQ: String)(implicit session: SparkSession) = {
    val neo4j = EncryptedNeo4j(session)
    val nodes = neo4j.cypher(nodeQ).loadNodeRdds.map(row => row(0).asInstanceOf[InternalNode])
    val rels = neo4j.cypher(relQ).loadRowRdd.map(row => row(0).asInstanceOf[InternalRelationship])

    nodes -> rels
  }

  case class LoadingContext(schema: Schema, globals: GlobalsRegistry)

  private def createSpace(nodes: RDD[InternalNode], rels: RDD[InternalRelationship],
                          sourceNode: String = "source", rel: String = "rel", targetNode: String = "target")
                         (implicit sparkSession: SparkSession, context: LoadingContext): SparkGraphSpace = {

    val nodeFields = (name: String, cypherType: CTNode) => computeNodeFields(name, cypherType)
    val nodeHeader = (name: String, cypherType: CTNode) => nodeFields(name, cypherType).map(_._1).foldLeft(RecordHeader.empty) {
      case (acc, next) => acc.update(addContent(next))._1
    }
    val nodeStruct = (name: String, cypherType: CTNode) => StructType(nodeFields(name, cypherType).map(_._2).toArray)
    val nodeRDD = (name: String, cypherType: CTNode) => nodes
      .filter(filterNode(cypherType))
      .map(nodeToRow(nodeHeader(name, cypherType), nodeStruct(name, cypherType)))

    val nodeFrame = (name: String, cypherType: CTNode) => {
      val slot = nodeHeader(name, cypherType).slotFor(Var(name)(cypherType))
      val df = sparkSession.createDataFrame(nodeRDD(name, cypherType), nodeStruct(name, cypherType))
      val col = df.col(df.columns(slot.index))
      df.repartition(col).sortWithinPartitions(col).cache()
    }

    val relFields = (name: String, cypherType: CTRelationship) => computeRelFields(name, cypherType)
    val relHeader = (name: String, cypherType: CTRelationship) => relFields(name, cypherType).map(_._1).foldLeft(RecordHeader.empty) {
      case (acc, next) => acc.update(addContent(next))._1
    }
    val relStruct = (name: String, cypherType: CTRelationship) => StructType(relFields(name, cypherType).map(_._2).toArray)
    val relRDD = (name: String, cypherType: CTRelationship) => rels
      .filter(filterRel(cypherType))
      .map(relToRow(relHeader(name, cypherType), relStruct(name, cypherType)))

    val relFrame = (name: String, cypherType: CTRelationship) => {
      val slot = relHeader(name, cypherType).slotFor(Var(name)(cypherType))
      val df = sparkSession.createDataFrame(relRDD(name, cypherType), relStruct(name, cypherType)).cache()
      val col = df.col(df.columns(slot.index))
      df.repartition(col).sortWithinPartitions(col).cache()
    }

    new SparkGraphSpace with Serializable {
      selfSpace =>

      override def base = new SparkCypherGraph with Serializable {
        selfBase =>

        override def nodes(name: String, cypherType: CTNode) =
          SparkCypherRecords.create(nodeHeader(name, cypherType), nodeFrame(name, cypherType))(selfSpace)

        override def relationships(name: String, cypherType: CTRelationship) =
          SparkCypherRecords.create(relHeader(name, cypherType), relFrame(name, cypherType))(selfSpace)

        override def space: SparkGraphSpace = selfSpace

        override def schema: Schema = context.schema
      }

      override def session = sparkSession
      override var tokens = SparkCypherRecordsTokens(context.globals.tokens)
    }
  }

  private def computeNodeFields(name: String, cypherType: CTNode)(implicit context: LoadingContext): Seq[(SlotContent, StructField)] = {
    val node = Var(name)(cypherType)

    val schema = context.schema
    val tokens = context.globals.tokens

    val labels = if (cypherType.labels.values.exists(_ == true)) cypherType.labels.filter(_._2).keySet else schema.labels

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

  private def computeRelFields(name: String, cypherType: CTRelationship)(implicit context: LoadingContext): Seq[(SlotContent, StructField)] = {
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
    val typeSlot = ProjectedExpr(TypeId(rel)(CTInteger))
    val typeField = StructField(SparkColumnName.of(typeSlot), IntegerType, nullable = false)

    val idSlot = OpaqueField(rel)
    val idField = StructField(SparkColumnName.of(idSlot), LongType, nullable = false)

    val sourceSlot = ProjectedExpr(StartNode(rel)(CTNode))
    val sourceField = StructField(SparkColumnName.of(sourceSlot), LongType, nullable = false)
    val targetSlot = ProjectedExpr(EndNode(rel)(CTNode))
    val targetField = StructField(SparkColumnName.of(targetSlot), LongType, nullable = false)

    Seq(sourceSlot -> sourceField, idSlot -> idField,
      typeSlot -> typeField, targetSlot -> targetField) ++ propertyFields
  }

  private case class filterNode(nodeDef: CTNode)
                               (implicit context: LoadingContext) extends (InternalNode => Boolean) {

    val requiredLabels: Set[String] = nodeDef.labels.filter(_._2).keySet

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

      val keys = labels.map(l => graphSchema.nodeKeys(l)).reduce(_ ++ _)

      val values = header.slots.map { s =>
        s.content.key match {
          case Property(_, PropertyKey(keyName)) =>
            val propValue = keys.get(keyName) match {
              case Some(t) if t == s.content.cypherType => props.get(keyName).orNull
              case _ => null
            }
            sparkValue(schema(s.index).dataType, propValue)
          case HasLabel(_, label) =>
            labels(label.name)
          case _: Var =>
            importedNode.id()

          case _ => ??? // nothing else should appear
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

      val keys = graphSchema.relationshipKeys(relType)

      val values = header.slots.map { s =>
        s.content.key match {
          case Property(_, PropertyKey(keyName)) =>
            val propValue = keys.get(keyName) match {
              case Some(t) if t == s.content.cypherType => props.get(keyName).orNull
              case _ => null
            }
            sparkValue(schema(s.index).dataType, propValue)

          case _: StartNode =>
            importedRel.startNodeId()

          case _: EndNode =>
            importedRel.endNodeId()

          case _: TypeId =>
            tokens.relTypeRefByName(relType).id

          case _: Var =>
            importedRel.id()

          case x => throw new IllegalArgumentException(s"Header contained unexpected expression: $x")
        }
      }

      Row(values: _*)
    }
  }

  private def sparkValue(typ: DataType, value: AnyRef): Any = typ match {
    case StringType | LongType | BooleanType | DoubleType => value
    case BinaryType => if (value == null) null else value.toString.getBytes // TODO: Call kryo
    case _ => CypherValue(value)
  }

  def configureNeo4jAccess(config: SparkConf)(url: String, user: String = "", pw: String = ""): SparkConf = {
    if (url.nonEmpty) config.set("spark.neo4j.bolt.url", url)
    if (user.nonEmpty) config.set("spark.neo4j.bolt.user", user)
    if (pw.nonEmpty) config.set("spark.neo4j.bolt.password", pw) else config
  }
}
