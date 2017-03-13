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
import org.opencypher.spark.prototype.api.record.{ProjectedExpr, RecordHeader, SlotContent}
import org.opencypher.spark.prototype.api.schema.{Schema, VerifiedSchema}
import org.opencypher.spark.prototype.api.spark.{SparkCypherGraph, SparkCypherRecords, SparkCypherView, SparkGraphSpace}
import org.opencypher.spark.prototype.impl.syntax.header._

trait SparkGraphSpaceLoading {

  def fromNeo4j(verified: VerifiedSchema,
                nodeQuery: String = "CYPHER runtime=compiled MATCH (n) RETURN n",
                relQuery: String = "CYPHER runtime=compiled MATCH ()-[r]->() RETURN r")
               (implicit sc: SparkSession): SparkGraphSpace = {
    val neo4j = Neo4j(sc.sparkContext)
    val graphSchema = verified.schema

    val nodes = neo4j.cypher(nodeQuery).loadNodeRdds.map(row => row(0).asInstanceOf[InternalNode])
    val rels = neo4j.cypher(relQuery).loadRowRdd.map(row => row(0).asInstanceOf[InternalRelationship])

    val schemaGlobals = GlobalsRegistry.fromSchema(verified)

    val nodeFields = computeNodeFields(graphSchema, schemaGlobals)
    val nodeHeader = nodeFields.map(_._1).foldLeft(RecordHeader.empty) {
      case (acc, next) => acc.update(addContent(next))._1
    }
    val nodeStruct = StructType(nodeFields.map(_._2).toArray)
    val nodeRDD: RDD[Row] = nodes.map(nodeToRow(nodeHeader, nodeStruct, schemaGlobals))
    val nodeFrame = sc.createDataFrame(nodeRDD, nodeStruct)

    val nodeRecords = new SparkCypherRecords with Serializable {
      override def data = nodeFrame
      override def header = nodeHeader
    }

    val relFields = computeRelFields(graphSchema, schemaGlobals)
    val relStruct = StructType(relFields.map(_._3).toArray)
    val relRDD: RDD[Row] = rels.map(relToRow(relFields, schemaGlobals))
    val relFrame = sc.createDataFrame(relRDD, relStruct)

    val relRecords = new SparkCypherRecords with Serializable {
      override def data = relFrame
      override def header = ??? // RecordsHeader.from(constructHeader(relFields))
    }

    new SparkGraphSpace with Serializable {
      selfSpace =>

      override def base = new SparkCypherGraph with Serializable {
        selfBase =>
        override def nodes = new SparkCypherView with Serializable {
          override def domain = selfBase
          override def model = QueryModel[Expr](null, schemaGlobals, Map.empty)
          override def records = nodeRecords
          override def graph = ???
        }
        override def relationships = new SparkCypherView with Serializable {
          override def domain = selfBase
          override def model = QueryModel[Expr](null, schemaGlobals, Map.empty)
          override def records = relRecords
          override def graph = ???
        }
        override def constituents = ???
        override def space = selfSpace
        override def schema = graphSchema
      }
      override def globals = schemaGlobals
    }
  }

  private def constructHeader(stuff: Seq[(Expr, CypherType, StructField)]): Seq[SlotContent] = {
    stuff.map {
      case (expr, t, field) => ??? /// RecordSlot(ExprSlotKey(expr), t)
    }
  }

  private def computeNodeFields(schema: Schema, globals: GlobalsRegistry): Seq[(SlotContent, StructField)] = {
    val nodeVar = Var("n")
    val labelFields = schema.labels.map { name =>
      val label = HasLabel(nodeVar, globals.label(name))
      val slot = ProjectedExpr(label, CTBoolean)
      val field = StructField(SparkColumnName.of(slot), BooleanType, nullable = false)
      slot -> field
    }
    val propertyFields = schema.labels.flatMap { l =>
      schema.nodeKeys(l).map {
        case (name, t) =>
          val property = Property(nodeVar, globals.propertyKey(name))
          val slot = ProjectedExpr(property, t)
          val field = StructField(SparkColumnName.of(slot), sparkType(t), nullable = t.isNullable)
          (slot, field)
      }
    }
    val nodeSlot = ProjectedExpr(nodeVar, CTNode)
    val slotField = nodeSlot -> StructField(SparkColumnName.of(nodeSlot), LongType, nullable = false)
    Seq(slotField) ++ labelFields ++ propertyFields
  }

  private def computeRelFields(schema: Schema, globals: GlobalsRegistry): Seq[(Expr, CypherType, StructField)] = {
    val relVar = Var("r")
    val propertyFields = schema.relationshipTypes.flatMap { typ =>
      schema.relationshipKeys(typ).map {
        case (name, t) =>
          val property = Property(relVar, globals.propertyKey(name))
          val slot = ProjectedExpr(property, t)
          val field = StructField(SparkColumnName.of(slot), sparkType(t), nullable = t.isNullable)
          (property, t, field)
      }
    }
    val typeField = (TypeId(relVar), CTInteger, StructField("type", IntegerType, nullable = false))
    val idField = (relVar, CTInteger, StructField("r", LongType, nullable = false))
    Seq(idField, typeField) ++ propertyFields
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

  private case class relToRow(fieldMap: Seq[(Expr, CypherType, StructField)], globals: GlobalsRegistry) extends (InternalRelationship => Row) {
    override def apply(importedRel: InternalRelationship): Row = {

      import scala.collection.JavaConverters._

      val props = importedRel.asMap().asScala

      val values = fieldMap.map {
        case (Property(_, ref), _, field) =>
          val key = globals.propertyKey(ref).name
          val value = props.get(key).orNull
          sparkValue(field.dataType, value)

        case (TypeId(_), _, _) =>
          globals.relType(importedRel.`type`()).id

        case (Var(_), _, field) =>
          importedRel.id()
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
