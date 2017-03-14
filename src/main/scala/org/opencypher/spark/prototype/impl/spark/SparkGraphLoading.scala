package org.opencypher.spark.prototype.impl.spark

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.neo4j.driver.internal.{InternalNode, InternalRelationship}
import org.neo4j.spark.Neo4j
import org.opencypher.spark.api.CypherType
import org.opencypher.spark.api.types._
import org.opencypher.spark.benchmark.Converters.cypherValue
import org.opencypher.spark.prototype.api.expr._
import org.opencypher.spark.prototype.api.ir.QueryModel
import org.opencypher.spark.prototype.api.ir.global.GlobalsRegistry
import org.opencypher.spark.prototype.api.record.{OpaqueField, ProjectedExpr, RecordHeader, SlotContent}
import org.opencypher.spark.prototype.api.schema.{Schema, VerifiedSchema}
import org.opencypher.spark.prototype.api.spark.{SparkCypherGraph, SparkCypherRecords, SparkCypherView, SparkGraphSpace}
import org.opencypher.spark.prototype.impl.syntax.header._

trait SparkGraphLoading {

  def fromNeo4j(verified: VerifiedSchema,
                nodeQuery: String = "CYPHER runtime=compiled MATCH (n) RETURN n",
                relQuery: String = "CYPHER runtime=compiled MATCH ()-[r]->() RETURN r")
               (implicit sc: SparkSession): SparkGraphSpace = {
    val neo4j = Neo4j(sc.sparkContext)
    val graphSchema = verified.schema

    val nodes = neo4j.cypher(nodeQuery).loadNodeRdds.map(row => row(0).asInstanceOf[InternalNode])
    val rels = neo4j.cypher(relQuery).loadRowRdd.map(row => row(0).asInstanceOf[InternalRelationship])

    val schemaGlobals = GlobalsRegistry.fromSchema(verified)

    val nodeFields = (v: Var) => computeNodeFields(v, graphSchema, schemaGlobals)
    val nodeHeader = (v: Var) => nodeFields(v).map(_._1).foldLeft(RecordHeader.empty) {
      case (acc, next) => acc.update(addContent(next))._1
    }
    val nodeStruct = (v: Var) => StructType(nodeFields(v).map(_._2).toArray)
    val nodeRDD = (v: Var) => nodes.map(nodeToRow(nodeHeader(v), nodeStruct(v), schemaGlobals))
    val nodeFrame = (v: Var) => sc.createDataFrame(nodeRDD(v), nodeStruct(v))

    val nodeRecords = (v: Var) => new SparkCypherRecords with Serializable {
      override def data = nodeFrame(v)
      override def header = nodeHeader(v)
    }

    val relFields = (v: Var) => computeRelFields(v, graphSchema, schemaGlobals)
    val relHeader = (v: Var) => relFields(v).map(_._1).foldLeft(RecordHeader.empty) {
      case (acc, next) => acc.update(addContent(next))._1
    }
    val relStruct = (v: Var) => StructType(relFields(v).map(_._2).toArray)
    val relRDD = (v: Var) => rels.map(relToRow(relHeader(v), relStruct(v), schemaGlobals))
    val relFrame = (v: Var) => sc.createDataFrame(relRDD(v), relStruct(v))

    val relRecords = (v: Var) => new SparkCypherRecords with Serializable {
      override def data = relFrame(v)
      override def header = relHeader(v)
    }

    new SparkGraphSpace with Serializable {
      selfSpace =>

      override def base = new SparkCypherGraph with Serializable {
        selfBase =>
        override def nodes(v: Var) = new SparkCypherView with Serializable {
          override def domain = selfBase
          override def model = QueryModel[Expr](null, schemaGlobals, Map.empty)
          override def records = nodeRecords(v)
          override def graph = ???
        }
        override def relationships(v: Var) = new SparkCypherView with Serializable {
          override def domain = selfBase
          override def model = QueryModel[Expr](null, schemaGlobals, Map.empty)
          override def records = relRecords(v)
          override def graph = ???
        }
        override def constituents = ???
        override def space = selfSpace
        override def schema = graphSchema
      }
      override def globals = schemaGlobals
    }
  }

  private def computeNodeFields(node: Var, schema: Schema, globals: GlobalsRegistry): Seq[(SlotContent, StructField)] = {
    val labelFields = schema.labels.map { name =>
      val label = HasLabel(node, globals.label(name))
      val slot = ProjectedExpr(label, CTBoolean)
      val field = StructField(SparkColumnName.of(slot), BooleanType, nullable = false)
      slot -> field
    }
    val propertyFields = schema.labels.flatMap { l =>
      schema.nodeKeys(l).map {
        case (name, t) =>
          val property = Property(node, globals.propertyKey(name))
          val slot = ProjectedExpr(property, t)
          val field = StructField(SparkColumnName.of(slot), sparkType(t), nullable = t.isNullable)
          slot -> field
      }
    }
    val nodeSlot = OpaqueField(node, CTNode)
    val nodeField = StructField(SparkColumnName.of(nodeSlot), LongType, nullable = false)
    val slotField = nodeSlot -> nodeField
    Seq(slotField) ++ labelFields ++ propertyFields
  }

  private def computeRelFields(rel: Var, schema: Schema, globals: GlobalsRegistry): Seq[(SlotContent, StructField)] = {
    val propertyFields = schema.relationshipTypes.flatMap { typ =>
      schema.relationshipKeys(typ).map {
        case (name, t) =>
          val property = Property(rel, globals.propertyKey(name))
          val slot = ProjectedExpr(property, t)
          val field = StructField(SparkColumnName.of(slot), sparkType(t), nullable = t.isNullable)
          slot -> field
      }
    }
    val typeSlot = ProjectedExpr(TypeId(rel), CTInteger)
    val typeField = StructField(SparkColumnName.of(typeSlot), IntegerType, nullable = false)

    val idSlot = OpaqueField(rel, CTRelationship)
    val idField = StructField(SparkColumnName.of(idSlot), LongType, nullable = false)

    val sourceSlot = ProjectedExpr(StartNode(rel), CTNode)
    val sourceField = StructField(SparkColumnName.of(sourceSlot), LongType, nullable = false)
    val targetSlot = ProjectedExpr(EndNode(rel), CTNode)
    val targetField = StructField(SparkColumnName.of(targetSlot), LongType, nullable = false)

    Seq(sourceSlot -> sourceField, idSlot -> idField,
      typeSlot -> typeField, targetSlot -> targetField) ++ propertyFields
  }

  object sparkType extends Serializable {
    def apply(ct: CypherType): DataType = ct.material match {
      case CTString => StringType
      case CTInteger => LongType
      case CTBoolean => BooleanType
      case CTAny => BinaryType
      case x => throw new NotImplementedError(s"No mapping for $x")
    }
  }

  private case class nodeToRow(header: RecordHeader, schema: StructType, globals: GlobalsRegistry) extends (InternalNode => Row) {
    override def apply(importedNode: InternalNode): Row = {

      import scala.collection.JavaConverters._

      val props = importedNode.asMap().asScala
      val labels = importedNode.labels().asScala.toSet

      val values = header.slots.map { s =>
        s.content.key match {
          case Property(_, ref) =>
            val propValue = props.get(globals.propertyKey(ref).name).orNull
            sparkValue(schema(s.index).dataType, propValue)
          case HasLabel(_, ref) =>
            labels(globals.label(ref).name)
          case _: Var =>
            importedNode.id()

          case _ => ??? // nothing else should appear
        }
      }

      Row(values: _*)
    }
  }

  private case class relToRow(header: RecordHeader, schema: StructType, globals: GlobalsRegistry) extends (InternalRelationship => Row) {
    override def apply(importedRel: InternalRelationship): Row = {

      import scala.collection.JavaConverters._

      val props = importedRel.asMap().asScala

      val values = header.slots.map { s =>
        s.content.key match {
          case Property(_, ref) =>
            val propValue = props.get(globals.propertyKey(ref).name).orNull
            sparkValue(schema(s.index).dataType, propValue)

          case _: StartNode =>
            importedRel.startNodeId()

          case _: EndNode =>
            importedRel.endNodeId()

          case _: TypeId =>
            globals.relType(importedRel.`type`()).id

          case _: Var =>
            importedRel.id()
        }
      }

      Row(values: _*)
    }
  }

  private def sparkValue(typ: DataType, value: AnyRef): Any = typ match {
    case StringType | LongType | BooleanType => value
    case BinaryType => if (value == null) null else value.toString.getBytes // TODO: Call kryo
    case _ => cypherValue(value)
  }

  def configureNeo4jAccess(config: SparkConf)(url: String, user: String = "", pw: String = ""): SparkConf = {
    if (url.nonEmpty) config.set("spark.neo4j.bolt.url", url)
    if (user.nonEmpty) config.set("spark.neo4j.bolt.user", user)
    if (pw.nonEmpty) config.set("spark.neo4j.bolt.password", pw) else config
  }
}
