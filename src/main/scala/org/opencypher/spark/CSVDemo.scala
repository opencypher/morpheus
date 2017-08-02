package org.opencypher.spark

import java.util.Calendar

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.opencypher.spark.api.io.CsvGraphLoader
import org.opencypher.spark.api.ir.global.TokenRegistry
import org.opencypher.spark.api.spark.{SparkCypherGraph, SparkCypherResult, SparkGraphSpace}
import org.opencypher.spark.impl.instances.spark.cypher._
import org.opencypher.spark.impl.syntax.cypher._
import org.opencypher.spark_legacy.CypherKryoRegistrar
import org.opencypher.spark_legacy.benchmark.Configuration.{Logging, MasterAddress}

object CSVDemo {

  val conf = new SparkConf(true)
  conf.set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
  conf.set("spark.kryo.registrator", classOf[CypherKryoRegistrar].getCanonicalName)

  implicit val session = SparkSession.builder()
    .config(conf)
    .master(MasterAddress.get())
    .appName(s"cypher-for-apache-spark-benchmark-${Calendar.getInstance().getTime}")
    .getOrCreate()

  implicit val graphSpace = SparkGraphSpace.empty(session, TokenRegistry.empty)

  lazy val graph: SparkCypherGraph = new CsvGraphLoader(getClass.getResource("/demo/ldbc_1").getFile).load

  session.sparkContext.setLogLevel(Logging.get())

  def cypher(query: String): SparkCypherResult = {
    println(s"Now executing query: $query")

    val result: SparkCypherResult = graph.cypher(query)

    result.recordsWithDetails.toDF().cache()

    val start = System.currentTimeMillis()
    println(s"Returned ${result.records.toDF().count()} row(s) in ${System.currentTimeMillis() - start} ms")

    result
  }
}
