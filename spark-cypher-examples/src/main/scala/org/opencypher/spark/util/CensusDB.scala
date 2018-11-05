/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.spark.util

import java.sql.DriverManager

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.opencypher.spark.api.io.{JdbcFormat, ParquetFormat, StorageFormat}

object CensusDB {

  case class Input(
    table: String,
    csvPath: String,
    dfSchema: StructType
  )

  val townInput = Input(
    table = "TOWN",
    csvPath = "/census/csv/town.csv",
    dfSchema = toStructType(Seq(
      "CITY_NAME" -> StringType,
      "REGION" -> StringType))
  )

  val residentsInput = Input(
    table = "RESIDENTS",
    csvPath = "/census/csv/residents.csv",
    dfSchema = toStructType(Seq(
      "CITY_NAME" -> StringType,
      "FIRST_NAME" -> StringType,
      "LAST_NAME" -> StringType,
      "PERSON_NUMBER" -> StringType,
      "REGION" -> StringType))
  )

  val visitorsInput = Input(
    table = "VISITORS",
    csvPath = "/census/csv/visitors.csv",
    dfSchema = toStructType(Seq(
      "AGE" -> LongType,
      "CITY_NAME" -> StringType,
      "COUNTRY" -> StringType,
      "DATE_OF_ENTRY" -> StringType,
      "ENTRY_SEQUENCE" -> LongType,
      "FIRST_NAME" -> StringType,
      "ISO3166" -> StringType,
      "LAST_NAME" -> StringType,
      "PASSPORT_NUMBER" -> LongType,
      "REGION" -> StringType))
  )

  val licensedDogsInput = Input(
    table = "LICENSED_DOGS",
    csvPath = "/census/csv/licensed_dogs.csv",
    dfSchema = toStructType(Seq(
      "CITY_NAME" -> StringType,
      "LICENCE_DATE" -> StringType,
      "LICENCE_NUMBER" -> LongType,
      "NAME" -> StringType,
      "PERSON_NUMBER" -> StringType,
      "REGION" -> StringType
    ))
  )

  def toStructType(columns: Seq[(String, DataType)]): StructType = {
    val structFields = columns.map {
      case (columnName, dataType: DataType) => StructField(columnName, dataType, nullable = false)
    }
    StructType(structFields.toList)
  }

  def createJdbcData(driver: String, jdbcUrl: String, schema: String)(implicit sparkSession: SparkSession): Unit = {

    def executeDDL(ddlSeq: Seq[String]): Unit = {
      val conn = DriverManager.getConnection(jdbcUrl)
      conn.setSchema(schema)
      val stmt = conn.createStatement()
      ddlSeq.foreach(stmt.executeUpdate)
    }

    // Create schema/database
    Class.forName(driver)
    executeDDL(Seq(s"DROP SCHEMA IF EXISTS $schema", s"CREATE SCHEMA IF NOT EXISTS $schema", s"SET SCHEMA $schema"))

    // populate the data
    val baseOptions = Map("url" -> jdbcUrl, "driver" -> driver)
    populateData(townInput, JdbcFormat, schema, baseOptions ++ Map("dbtable" -> s"$schema.${townInput.table}"))
    populateData(residentsInput, JdbcFormat, schema, baseOptions ++ Map("dbtable" -> s"$schema.${residentsInput.table}"))
    populateData(visitorsInput, JdbcFormat, schema, baseOptions ++ Map("dbtable" -> s"$schema.${visitorsInput.table}"))
    populateData(licensedDogsInput, JdbcFormat, schema, baseOptions ++ Map("dbtable" -> s"$schema.${licensedDogsInput.table}"))

    // create the views
    executeDDL(Seq(
      viewPerson(schema),
      viewVisitor(schema),
      viewResident(schema),
      viewLicensedDog(schema),
      viewResidentEnumInTown(schema),
      viewVisitorEnumInTown(schema)))
  }

  def createHiveData(schema: String)(implicit sparkSession: SparkSession): Unit = {

    // Create the database
    sparkSession.sql(s"CREATE DATABASE $schema").count

    def executeDDL(ddlSeq: Seq[String]): Unit = {
      ddlSeq.foreach(sparkSession.sql(_).count)
    }

    populateData(townInput, ParquetFormat, schema)
    populateData(residentsInput, ParquetFormat, schema)
    populateData(visitorsInput, ParquetFormat, schema)
    populateData(licensedDogsInput, ParquetFormat, schema)

    // create the views
    executeDDL(Seq(
      viewPerson(schema),
      viewVisitor(schema),
      viewResident(schema),
      viewLicensedDog(schema),
      viewResidentEnumInTown(schema),
      viewVisitorEnumInTown(schema)))
  }

  private def viewPerson(schema: String): String =
    s"""
       |CREATE VIEW $schema.view_person AS
       |  SELECT
       |    first_name,
       |    last_name
       |  FROM
       |    $schema.residents
    """.stripMargin

  private def viewVisitor(schema: String): String =
    s"""
       |CREATE VIEW $schema.view_visitor AS
       |  SELECT
       |    first_name,
       |    last_name,
       |    iso3166 as nationality,
       |    passport_number,
       |    date_of_entry,
       |    entry_sequence as sequence,
       |    age
       |  FROM
       |    $schema.visitors
    """.stripMargin

  private def viewResident(schema: String): String =
    s"""
       |CREATE VIEW $schema.view_resident AS
       |  SELECT
       |    first_name,
       |    last_name,
       |    person_number
       |  FROM
       |    $schema.residents
    """.stripMargin

  private def viewLicensedDog(schema: String): String =
    s"""
       |CREATE VIEW $schema.view_licensed_dog AS
       |  SELECT
       |    person_number,
       |    licence_number,
       |    licence_date as date_of_licence,
       |    region,
       |    city_name
       |  FROM
       |    $schema.licensed_dogs
    """.stripMargin

  private def viewResidentEnumInTown(schema: String): String =
    s"""
       |CREATE VIEW $schema.view_resident_enumerated_in_town AS
       |  SELECT
       |    PERSON_NUMBER,
       |    REGION,
       |    CITY_NAME
       |  FROM
       |    $schema.residents
    """.stripMargin

  private def viewVisitorEnumInTown(schema: String): String =
    s"""
       |CREATE VIEW $schema.view_visitor_enumerated_in_town AS
       |  SELECT
       |    ISO3166 AS countryOfOrigin,
       |    PASSPORT_NUMBER AS PASSPORT_NO,
       |    REGION,
       |    CITY_NAME
       |  FROM
       |    $schema.visitors
    """.stripMargin

  private def populateData(
    input: Input,
    storageFormat: StorageFormat,
    dbSchema: String,
    options: Map[String, String] = Map.empty
  )(implicit sparkSession: SparkSession): Unit = {
    val writer = sparkSession
      .read
      .option("header", "true")
      .schema(input.dfSchema)
      .csv(getClass.getResource(s"${input.csvPath}").getPath)
      .write
      .format(storageFormat.name)
      .options(options)
      .mode("ignore")

    if (storageFormat == JdbcFormat) {
      writer.save
    } else {
      writer.saveAsTable(s"$dbSchema.${input.table}")
    }
  }

}
