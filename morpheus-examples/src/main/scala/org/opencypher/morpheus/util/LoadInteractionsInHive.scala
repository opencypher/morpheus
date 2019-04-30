/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.morpheus.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.opencypher.morpheus.api.MorpheusSession

object LoadInteractionsInHive {

  val databaseName = "customers"
  val baseTableName = s"$databaseName.csv_input"

  def load(show: Boolean = false)(implicit session: MorpheusSession): DataFrame = {

    val datafile = getClass.getResource("/customer-interactions/csv/customer-interactions.csv").toURI.getPath
    val structType = StructType(Seq(
      StructField("interactionId", LongType, nullable = false),
      StructField("date", StringType, nullable = false),
      StructField("customerIdx", LongType, nullable = false),
      StructField("empNo", LongType, nullable = false),
      StructField("empName", StringType, nullable = false),
      StructField("type", StringType, nullable = false),
      StructField("outcomeScore", StringType, nullable = false),
      StructField("accountHolderId", StringType, nullable = false),
      StructField("policyAccountNumber", StringType, nullable = false),
      StructField("customerId", StringType, nullable = false),
      StructField("customerName", StringType, nullable = false)
    ))

    val baseTable: DataFrame = session.sparkSession.read
      .format("csv")
      .option("header", "true")
      .schema(structType)
      .load(datafile)

    if (show) baseTable.show()

    session.sql(s"DROP DATABASE IF EXISTS $databaseName CASCADE")
    session.sql(s"CREATE DATABASE $databaseName")

    baseTable.write.saveAsTable(s"$baseTableName")

    // Create views for nodes
    createView(baseTableName, "interactions", true, "interactionId", "date", "type", "outcomeScore")
    createView(baseTableName, "customers", true, "customerIdx", "customerId", "customerName")
    createView(baseTableName, "account_holders", true, "accountHolderId")
    createView(baseTableName, "policies", true, "policyAccountNumber")
    createView(baseTableName, "customer_reps", true, "empNo", "empName")

    // Create views for relationships
    createView(baseTableName, "has_customer_reps", false, "interactionId", "empNo")
    createView(baseTableName, "has_customers", false, "interactionId", "customerIdx")
    createView(baseTableName, "has_policies", false, "interactionId", "policyAccountNumber")
    createView(baseTableName, "has_account_holders", false, "interactionId", "accountHolderId")

    baseTable
  }

  def createView(fromTable: String, viewName: String, distinct: Boolean, columns: String*)
    (implicit session: MorpheusSession): Unit = {
    val distinctString = if (distinct) "DISTINCT" else ""

    session.sql(
      s"""
         |CREATE VIEW $databaseName.${viewName}_SEED AS
         | SELECT $distinctString ${columns.mkString(", ")}
         | FROM $fromTable
         | WHERE date < '2017-01-01'
      """.stripMargin)

    session.sql(
      s"""
         |CREATE VIEW $databaseName.${viewName}_DELTA AS
         | SELECT $distinctString ${columns.mkString(", ")}
         | FROM $fromTable
         | WHERE date >= '2017-01-01'
      """.stripMargin)
  }

}