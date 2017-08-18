package org.opencypher.caps.demo

import java.util.Calendar

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.opencypher.caps.api.io.CsvGraphLoader
import org.opencypher.caps.api.ir.global.TokenRegistry
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSResult, SparkGraphSpace}
import org.opencypher.caps.impl.instances.spark.cypher._
import org.opencypher.caps.impl.syntax.cypher._
import Configuration.{Logging, MasterAddress}

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

  lazy val graph: CAPSGraph = new CsvGraphLoader(getClass.getResource("/demo/ldbc_1").getFile).load

  session.sparkContext.setLogLevel(Logging.get())

  def cypher(query: String): CAPSResult = {
    println(s"Now executing query: $query")

    val result: CAPSResult = graph.cypher(query)

    result.recordsWithDetails.toDF().cache()

    val start = System.currentTimeMillis()
    println(s"Returned ${result.records.toDF().count()} row(s) in ${System.currentTimeMillis() - start} ms")

    result
  }
}
