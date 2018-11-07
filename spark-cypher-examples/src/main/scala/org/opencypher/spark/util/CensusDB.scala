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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.spark.api.io.sql.{SqlDataSourceConfig, SqlDataSourceConfigException}
import org.opencypher.spark.api.io.{HiveFormat, JdbcFormat}
import org.opencypher.spark.testing.utils.H2Utils._

import scala.io.Source
import scala.util.Properties

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
      "REGION" -> StringType))
  )

  def toStructType(columns: Seq[(String, DataType)]): StructType = {
    val structFields = columns.map {
      case (columnName, dataType: DataType) => StructField(columnName, dataType, nullable = false)
    }
    StructType(structFields.toList)
  }

  private def readResourceAsString(name: String): String =
    Source.fromFile(getClass.getResource(name).toURI)
      .getLines()
      .filterNot(line => line.startsWith("#") || line.startsWith("CREATE INDEX"))
      .mkString(Properties.lineSeparator)

  val databaseName: String = "CENSUS"

  val createViewsSql: String = readResourceAsString("/census/sql/census_views.sql")

  def createJdbcData(sqlDataSourceConfig: SqlDataSourceConfig)(implicit sparkSession: SparkSession): Unit = {

    // Populate the data
    populateData(townInput, sqlDataSourceConfig)
    populateData(residentsInput, sqlDataSourceConfig)
    populateData(visitorsInput, sqlDataSourceConfig)
    populateData(licensedDogsInput, sqlDataSourceConfig)

    // Create the views
    withConnection(sqlDataSourceConfig) { connection =>
      connection.setSchema(databaseName)
      connection.execute(createViewsSql)
    }
  }

  def createHiveData(sqlDataSourceConfig: SqlDataSourceConfig)(implicit sparkSession: SparkSession): Unit = {

    // Create the database
    sparkSession.sql(s"CREATE DATABASE CENSUS").count

    // Populate the data
    populateData(townInput, sqlDataSourceConfig)
    populateData(residentsInput, sqlDataSourceConfig)
    populateData(visitorsInput, sqlDataSourceConfig)
    populateData(licensedDogsInput, sqlDataSourceConfig)

    // Create the views
    createViewsSql.split(";").foreach(sparkSession.sql)
  }

  private def populateData(input: Input, cfg: SqlDataSourceConfig)(implicit sparkSession: SparkSession): Unit = {
    val writer = sparkSession
      .read
      .option("header", "true")
      .schema(input.dfSchema)
      .csv(getClass.getResource(s"${input.csvPath}").getPath)
      .write
      .format(cfg.storageFormat.name)
      .mode("ignore")

    if (cfg.storageFormat == JdbcFormat) {
      writer
        .option("url", cfg.jdbcUri.getOrElse(throw SqlDataSourceConfigException("Missing JDBC URI")))
        .option("driver", cfg.jdbcDriver.getOrElse(throw SqlDataSourceConfigException("Missing JDBC Driver")))
        .option("fetchSize", cfg.jdbcFetchSize)
        .option("dbtable", s"$databaseName.${input.table}")
        .save
    } else if (cfg.storageFormat == HiveFormat){
      writer.saveAsTable(s"$databaseName.${input.table}")
    } else {
      throw IllegalArgumentException(s"${HiveFormat.name} or ${JdbcFormat.name}", cfg.storageFormat.name)
    }
  }
}
