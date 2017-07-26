package org.opencypher.spark

import java.util.Calendar

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.opencypher.spark.api.ir.global.TokenRegistry
import org.opencypher.spark.api.record.{NodeScan, RelationshipScan}
import org.opencypher.spark.api.spark.{SparkCypherGraph, SparkCypherRecords, SparkCypherResult, SparkGraphSpace}
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

  lazy val graph = {
    // (1) Load node CSV files (by label)
    val `:Person` =
      NodeScan.on("n0" -> "ID") {
        _.build
          .withImpliedLabel("Person")
          .withOptionalLabel("Male" -> "IS_MALE")
          .withOptionalLabel("Female" -> "IS_FEMALE")
          .withPropertyKey("firstName" -> "FIRST_NAME")
          .withPropertyKey("lastName" -> "LAST_NAME")
          .withPropertyKey("createdAt" -> "CREATED_AT")
      }.from(loadCSV("/demo/ldbc_1/nodes/person.csv", StructType(Array(
        StructField("ID", LongType, false),
        StructField("FIRST_NAME", StringType, false),
        StructField("LAST_NAME", StringType, false),
        StructField("IS_MALE", BooleanType, false),
        StructField("IS_FEMALE", BooleanType, false),
        StructField("CREATED_AT", LongType, false)
      ))))

    val `:University` =
      NodeScan.on("n1" -> "ID") {
        _.build
          .withImpliedLabel("University")
          .withPropertyKey("name" -> "NAME")
      }.from(loadCSV("/demo/ldbc_1/nodes/university.csv", StructType(Array(
        StructField("ID", LongType, false),
        StructField("NAME", StringType, false)
      ))))

    val `:Company` =
      NodeScan.on("n2" -> "ID") {
        _.build
          .withImpliedLabel("Company")
          .withPropertyKey("name" -> "NAME")
      }.from(loadCSV("/demo/ldbc_1/nodes/company.csv", StructType(Array(
        StructField("ID", LongType, false),
        StructField("NAME", StringType, false)
      ))))

    val `:City` =
      NodeScan.on("n3" -> "ID") {
        _.build
          .withImpliedLabel("City")
          .withPropertyKey("name" -> "NAME")
      }.from(loadCSV("/demo/ldbc_1/nodes/city.csv", StructType(Array(
        StructField("ID", LongType, false),
        StructField("NAME", StringType, false)
      ))))

    val `:Country` =
      NodeScan.on("n4" -> "ID") {
        _.build
          .withImpliedLabel("Country")
          .withPropertyKey("name" -> "NAME")
      }.from(loadCSV("/demo/ldbc_1/nodes/country.csv", StructType(Array(
        StructField("ID", LongType, false),
        StructField("NAME", StringType, false)
      ))))

    val `:Continent` =
      NodeScan.on("n5" -> "ID") {
        _.build
          .withImpliedLabel("Continent")
          .withPropertyKey("name" -> "NAME")
      }.from(loadCSV("/demo/ldbc_1/nodes/continent.csv", StructType(Array(
        StructField("ID", LongType, false),
        StructField("NAME", StringType, false)
      ))))

    val `:Tag` =
      NodeScan.on("n6" -> "ID") {
        _.build
          .withImpliedLabel("Tag")
          .withPropertyKey("name" -> "NAME")
      }.from(loadCSV("/demo/ldbc_1/nodes/tag.csv", StructType(Array(
        StructField("ID", LongType, false),
        StructField("NAME", StringType, false)
      ))))

    // (2) Load edge CSV files (by type)
    val `:KNOWS` =
      RelationshipScan.on("e0" -> "ID") {
        _.from("SRC").to("DST").relType("KNOWS")
          .build
      }.from(loadCSV("/demo/ldbc_1/rels/knows.csv", StructType(Array(
        StructField("SRC", LongType, false),
        StructField("ID", LongType, false),
        StructField("DST", LongType, false)
      ))))

    val `:STUDY_AT` =
      RelationshipScan.on("e1" -> "ID") {
        _.from("SRC").to("DST").relType("STUDY_AT")
          .build
          .withPropertyKey("classYear" -> "CLASS_YEAR")
      }.from(loadCSV("/demo/ldbc_1/rels/studyAt.csv", StructType(Array(
        StructField("SRC", LongType, false),
        StructField("ID", LongType, false),
        StructField("DST", LongType, false),
        StructField("CLASS_YEAR", IntegerType, false)
      ))))

    val `:WORK_AT` =
      RelationshipScan.on("e2" -> "ID") {
        _.from("SRC").to("DST").relType("WORK_AT")
          .build
          .withPropertyKey("workFrom" -> "WORK_FROM")
      }.from(loadCSV("/demo/ldbc_1/rels/workAt.csv", StructType(Array(
        StructField("SRC", LongType, false),
        StructField("ID", LongType, false),
        StructField("DST", LongType, false),
        StructField("WORK_FROM", IntegerType, false)
      ))))

    val `:IS_LOCATED_IN` =
      RelationshipScan.on("e3" -> "ID") {
        _.from("SRC").to("DST").relType("IS_LOCATED_IN")
          .build
      }.from(loadCSV("/demo/ldbc_1/rels/isLocatedIn.csv", StructType(Array(
        StructField("SRC", LongType, false),
        StructField("ID", LongType, false),
        StructField("DST", LongType, false)
      ))))

    val `:IS_PART_OF` =
      RelationshipScan.on("e4" -> "ID") {
        _.from("SRC").to("DST").relType("IS_PART_OF")
          .build
      }.from(loadCSV("/demo/ldbc_1/rels/isPartOf.csv", StructType(Array(
        StructField("SRC", LongType, false),
        StructField("ID", LongType, false),
        StructField("DST", LongType, false)
      ))))

    val `:HAS_INTEREST` =
      RelationshipScan.on("e5" -> "ID") {
        _.from("SRC").to("DST").relType("HAS_INTEREST")
          .build
      }.from(loadCSV("/demo/ldbc_1/rels/hasInterest.csv", StructType(Array(
        StructField("SRC", LongType, false),
        StructField("ID", LongType, false),
        StructField("DST", LongType, false)
      ))))

    SparkCypherGraph.create(
      // node scans
      `:Person`, `:University`, `:Company`, `:City`, `:Country`, `:Continent`, `:Tag`,
      // rel scans
      `:KNOWS`, `:STUDY_AT`, `:WORK_AT`, `:IS_LOCATED_IN`, `:IS_PART_OF`, `:HAS_INTEREST`)
  }

  session.sparkContext.setLogLevel(Logging.get())

  def cypher(query: String): SparkCypherResult = {
    println(s"Now executing query: $query")

    val result: SparkCypherResult = graph.cypher(query)

    result.recordsWithDetails.toDF().cache()

    val start = System.currentTimeMillis()
    println(s"Returned ${result.records.toDF().count()} row(s) in ${System.currentTimeMillis() - start} ms")

    result
  }

  def loadCSV(file: String)(implicit sc: SparkSession): SparkCypherRecords = {
    val df = sc.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(getClass.getResource(file).getFile)

    SparkCypherRecords.create(df)
  }

  def loadCSV(file: String, schema: StructType)(implicit sc: SparkSession): SparkCypherRecords = {
    val df = sc.read
      .schema(schema)
      .csv(getClass.getResource(file).getFile)

    SparkCypherRecords.create(df)
  }
}
