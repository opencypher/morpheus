package org.neo4j.spark

import java.util

import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import scala.collection.JavaConverters._

/**
  * @author mh
  * @since 19.03.16
  */
object Neo4jJavaIntegration {
  def rowRDD(sc: SparkContext, query: String, parameters:java.util.Map[String,AnyRef]) =
    new Neo4jRowRDD(sc, query, if (parameters==null) Seq.empty else parameters.asScala.toSeq).toJavaRDD()

  def tupleRDD(sc: SparkContext, query: String, parameters: java.util.Map[String, AnyRef]) : JavaRDD[util.Map[String,AnyRef]] = {
    val params = if (parameters == null) Seq.empty else parameters.asScala.toSeq
    Neo4jTupleRDD(sc, query, params)
      .map((t) => new util.LinkedHashMap[String, AnyRef](t.toMap.asJava).asInstanceOf[util.Map[String,AnyRef]])
      .toJavaRDD()
  }

  def dataFrame(sqlContext: SQLContext, query: String, parameters: java.util.Map[String, AnyRef], schemaInfo: util.Map[String, String]) = {
    Neo4jDataFrame(sqlContext, query, parameters.asScala.toSeq, schemaInfo.asScala.toSeq:_*)
  }
}
