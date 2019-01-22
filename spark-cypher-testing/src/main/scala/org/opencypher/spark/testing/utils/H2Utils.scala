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
package org.opencypher.spark.testing.utils

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import org.apache.spark.sql._
import org.opencypher.spark.api.io.sql.{SqlDataSourceConfig, SqlDataSourceConfigException}

object H2Utils {

  implicit class ConnOps(conn: Connection) {
    def run[T](code: Statement => T): T = {
      val stmt = conn.createStatement()
      try { code(stmt) } finally { stmt.close() }
    }
    def execute(sql: String): Boolean = conn.run(_.execute(sql))
    def query(sql: String): ResultSet = conn.run(_.executeQuery(sql))
    def update(sql: String): Int = conn.run(_.executeUpdate(sql))
  }

  def withConnection[T](cfg: SqlDataSourceConfig.Jdbc)(code: Connection => T): T = {
    Class.forName(cfg.driver)
    val conn = (cfg.options.get("user"), cfg.options.get("password")) match {
      case (Some(user), Some(pass)) =>
        DriverManager.getConnection(cfg.url, user, pass)
      case _ =>
        DriverManager.getConnection(cfg.url)
    }
    try { code(conn) } finally { conn.close() }
  }

  implicit class DataFrameWriterOps(write: DataFrameWriter[Row]) {
    def maybeOption(key: String, value: Option[String]): DataFrameWriter[Row] =
      value.fold(write)(write.option(key, _))
  }

  implicit class DataFrameSqlOps(df: DataFrame) {

    def saveAsSqlTable(cfg: SqlDataSourceConfig.Jdbc, tableName: String): Unit =
      df.write
        .mode(SaveMode.Overwrite)
        .format("jdbc")
        .option("url", cfg.url)
        .option("driver", cfg.driver)
        .options(cfg.options)
        .option("dbtable", tableName)
        .save()
  }
}
