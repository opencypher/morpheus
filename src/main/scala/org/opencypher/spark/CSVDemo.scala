package org.opencypher.spark

import java.util.Calendar

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.opencypher.spark.api.ir.global.TokenRegistry
import org.opencypher.spark.api.record.{NodeScan, RelationshipScan}
import org.opencypher.spark.api.spark.{SparkCypherGraph, SparkCypherRecords, SparkGraphSpace}
import org.opencypher.spark_legacy.CypherKryoRegistrar
import org.opencypher.spark_legacy.benchmark.Configuration.{Logging, MasterAddress}
import org.opencypher.spark.impl.instances.spark.cypher._
import org.opencypher.spark.impl.syntax.cypher._

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

  session.sparkContext.setLogLevel(Logging.get())

  def main(args: Array[String]): Unit = {

    // (1) Load node CSV files (by label)
    val `:Person` =
      NodeScan.on("p" -> "ID") {
        _.build
          .withImpliedLabel("Person")
          .withOptionalLabel("IsEvil" -> "IS_EVIL")
          .withPropertyKey("name" -> "NAME")
          .withPropertyKey("yob" -> "YOB")
      }.from(loadCSV("/demo/dummy/persons.csv"))

    // (2) Load edge CSV files (by type)
    val `:KNOWS` =
      RelationshipScan.on("k" -> "ID") {
        _.from("SRC").to("DST").relType("KNOWS")
          .build
          .withPropertyKey("since" -> "SINCE")
      }.from(loadCSV("/demo/dummy/knows.csv"))

    val graph = SparkCypherGraph.create(`:Person`, `:KNOWS`)

    val res = graph.cypher("MATCH (n:Person) RETURN n.name")

    res.showRecords()
  }

  def loadCSV(file: String)(implicit sc: SparkSession): SparkCypherRecords = {
    val df = sc.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(getClass.getResource(file).getFile)

    SparkCypherRecords.create(df)
  }
}
