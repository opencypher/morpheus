package org.neo4j.spark

import java.util

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, SQLContext, types}
import org.neo4j.driver.internal.types.InternalTypeSystem
import org.neo4j.driver.v1._
import org.neo4j.driver.v1.types.{Type, TypeSystem}

import scala.collection.JavaConverters._

object Neo4jDataFrame {

  def mergeEdgeList(sc: SparkContext, dataFrame: DataFrame, source: (String,Seq[String]), relationship: (String,Seq[String]), target: (String,Seq[String])): Unit = {
    val mergeStatement = s"""
      UNWIND {rows} as row
      MERGE (source:`${source._1}` {`${source._2.head}` : row.source.`${source._2.head}`}) ON CREATE SET source += row.source
      MERGE (target:`${target._1}` {`${target._2.head}` : row.target.`${target._2.head}`}) ON CREATE SET target += row.target
      MERGE (source)-[rel:`${relationship._1}`]->(target) ON CREATE SET rel += row.relationship
      """
    val partitions = Math.max(1,(dataFrame.count() / 10000).asInstanceOf[Int])
    val config = Neo4jConfig(sc.getConf)
    dataFrame.repartition(partitions).foreachPartition( rows => {
      val params: AnyRef = rows.map(r =>
        Map(
          "source" -> source._2.map( c => (c, r.getAs[AnyRef](c))).toMap.asJava,
          "target" -> target._2.map( c => (c, r.getAs[AnyRef](c))).toMap.asJava,
          "relationship" -> relationship._2.map( c => (c, r.getAs[AnyRef](c))).toMap.asJava)
          .asJava).asJava
          execute(config, mergeStatement, Map("rows" -> params).asJava)
    })
  }

  def execute(config : Neo4jConfig, query: String, parameters: java.util.Map[String, AnyRef]) = {
    val driver: Driver = config.driver()
    val session = driver.session()
    try {
      session.run(query, parameters).consume()
    } finally {
      if (session.isOpen) session.close()
      driver.close()
    }
  }

  def withDataType(sqlContext: SQLContext, query: String, parameters: Seq[(String, Any)], schema: (String, types.DataType)*) = {
    val rowRdd = Neo4jRowRDD(sqlContext.sparkContext, query, parameters)
    sqlContext.createDataFrame(rowRdd, CypherTypes.schemaFromDataType(schema))
  }

  def apply(sqlContext: SQLContext, query: String, parameters: Seq[(String, Any)], schema: (String, String)*) = {
    val rowRdd = Neo4jRowRDD(sqlContext.sparkContext, query, parameters)
    sqlContext.createDataFrame(rowRdd, CypherTypes.schemaFromNamedType(schema))
  }

  def apply(sqlContext: SQLContext, query: String, parameters: java.util.Map[String, AnyRef]) = {
    val config = Neo4jConfig(sqlContext.sparkContext.getConf)
    val driver: Driver = config.driver()
    val session = driver.session()
    try {
      val result = session.run(query, parameters)
      if (!result.hasNext) throw new RuntimeException("Can't determine schema from empty result")
      val peek: Record = result.peek()
      val fields = peek.keys().asScala.map(k => (k, peek.get(k).`type`())).map(keyType => CypherTypes.field(keyType))
      val schema = StructType(fields)
      val rowRdd = new Neo4jResultRdd(sqlContext.sparkContext, result.asScala, peek.size(), session, driver)
      sqlContext.createDataFrame(rowRdd, schema)
    } finally {
      if (session.isOpen) session.close()
      driver.close()
    }
  }


  class Neo4jResultRdd(@transient sc: SparkContext, result : Iterator[Record], keyCount : Int, session: Session, driver:Driver)
  extends RDD[Row](sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    result.map((record) => {
      val res = keyCount match {
        case 0 => Row.empty
        case 1 => Row(record.get(0).asObject())
        case _ =>
          val builder = Seq.newBuilder[AnyRef]
          builder.sizeHint(keyCount)
          var i = 0
          while (i < keyCount) {
            builder += record.get(i).asObject()
            i = i + 1
          }
          Row.fromSeq(builder.result())
      }
      if (!result.hasNext) {
        session.close()
        driver.close()
      }
      res
    })
  }

  override protected def getPartitions: Array[Partition] = Array(new Neo4jPartition())
}

}

object CypherTypes {
  val INTEGER = types.LongType
  val FlOAT = types.DoubleType
  val STRING = types.StringType
  val BOOLEAN = types.BooleanType
  val NULL = types.NullType

  def apply(typ:String) = typ.toUpperCase match {
    case "LONG" => INTEGER
    case "INT" => INTEGER
    case "INTEGER" => INTEGER
    case "FLOAT" => FlOAT
    case "DOUBLE" => FlOAT
    case "NUMERIC" => FlOAT
    case "STRING" => STRING
    case "BOOLEAN" => BOOLEAN
    case "BOOL" => BOOLEAN
    case "NULL" => NULL
    case _ => STRING
  }
//  val MAP = edges.MapType(edges.StringType,edges.AnyDataType)
//  val LIST = edges.ArrayType(edges.AnyDataType)
// , MAP, LIST, NODE, RELATIONSHIP, PATH


  def toSparkType(typeSystem : TypeSystem, typ : Type): org.apache.spark.sql.types.DataType =
    if (typ == typeSystem.BOOLEAN()) CypherTypes.BOOLEAN
    else if (typ == typeSystem.STRING()) CypherTypes.STRING
    else if (typ == typeSystem.INTEGER()) CypherTypes.INTEGER
    else if (typ == typeSystem.FLOAT()) CypherTypes.FlOAT
    else if (typ == typeSystem.NULL()) CypherTypes.NULL
    else CypherTypes.STRING
  //    type match {
  //    case typeSystem.MAP => edges.MapType(edges.StringType,edges.ObjectType)
  //    case typeSystem.LIST => edges.ArrayType(edges.ObjectType, containsNull = false)
  //    case typeSystem.NODE => edges.VertexType
  //    case typeSystem.RELATIONSHIP => edges.EdgeType
  //    case typeSystem.PATH => edges.GraphType
  //    case _ => edges.StringType
  //  }

  def field(keyType: (String, Type)): StructField = {
    StructField(keyType._1, CypherTypes.toSparkType(InternalTypeSystem.TYPE_SYSTEM, keyType._2))
  }
  def schemaFromNamedType(schemaInfo: Seq[(String, String)]) = {
    val fields = schemaInfo.map(field =>
      StructField(field._1, CypherTypes(field._2), nullable = true) )
    StructType(fields)
  }
  def schemaFromDataType(schemaInfo: Seq[(String, types.DataType)]) = {
    val fields = schemaInfo.map(field =>
      StructField(field._1, field._2, nullable = true) )
    StructType(fields)
  }

}
