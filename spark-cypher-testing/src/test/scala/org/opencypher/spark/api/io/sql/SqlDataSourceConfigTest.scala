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
package org.opencypher.spark.api.io.sql

import org.opencypher.spark.api.io.sql.SqlDataSourceConfig.{File, Hive, Jdbc}
import org.opencypher.spark.api.io.{CsvFormat, ParquetFormat}
import org.scalatest.Matchers

import scala.io.Source

class SqlDataSourceConfigTest extends org.scalatest.FunSpec with Matchers {

  private def roundTrip(cfg: SqlDataSourceConfig): SqlDataSourceConfig =
    SqlDataSourceConfig.fromJson(SqlDataSourceConfig.toJson(cfg))

  describe("parsing") {

    it("config to Json roundTrip") {

      List(
        Hive(),
        Jdbc("foo", "driv"),
        Jdbc("foo", "driv", Map("foo" -> "bar")),
        File(CsvFormat, None),
        File(CsvFormat, Some("/foo/bar"), Map("foo" -> "bar2"))
      ).foreach(cfg => roundTrip(cfg) shouldEqual cfg)

    }

    it("parses multiple SQL data sources") {
      val jsonString = Source.fromURL(getClass.getResource("/sql/sql-data-sources.json")).mkString
      val config = Map(
        "CENSUS_ORACLE" -> Jdbc(
          url = "jdbc:h2:mem:CENSUS.db;INIT=CREATE SCHEMA IF NOT EXISTS CENSUS;DB_CLOSE_DELAY=30;",
          driver = "org.h2.Driver",
          options = Map(
            "fetchSize" -> "100"
          )
        ),
        "ORACLE_X2" -> Jdbc(
          url = "jdbc:h2:mem:X2.db;INIT=CREATE SCHEMA IF NOT EXISTS X2;DB_CLOSE_DELAY=30;",
          driver = "org.h2.Driver",
          options = Map(
            "user" -> "ORACLE_X2_USER",
            "password" -> "ORACLE_X2_PASS",
            "fetchSize" -> "10"
          )
        ),
        "HIVE_CENSUS" -> Hive(),
        "HIVE_X2" -> Hive(),
        "MY_DIR" -> File(
          format = ParquetFormat,
          basePath = Some("/my/path")
        )
      )
      SqlDataSourceConfig.dataSourcesFromString(jsonString) shouldEqual config

    }
  }

}
