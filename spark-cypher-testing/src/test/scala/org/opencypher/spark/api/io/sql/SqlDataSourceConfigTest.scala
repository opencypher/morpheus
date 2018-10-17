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
package org.opencypher.spark.api.io.sql

import SqlDataSourceConfig._
import org.scalatest.Matchers

import scala.io.Source

class SqlDataSourceConfigTest extends org.scalatest.FunSpec with Matchers {

  describe("parsing") {
    it("parses to and from JSON") {
      val ds = SqlDataSourceConfig("storageFormat", "dataSource", Some("schema"), Some("jdbcUri"), Some("jdbcDriver"), 42)
      val jsonString = ds.toJson
      fromJson(jsonString) shouldEqual ds
    }

    it("parses to and from JSON with missing values") {
      val ds = SqlDataSourceConfig("storageFormat", "dataSource")
      val jsonString = ds.toJson
      fromJson(jsonString) shouldEqual ds
    }

    it("parses multiple SQL data sources") {
      val jsonString = Source.fromURL(getClass.getResource("/sql/sql-data-sources.json")).getLines().mkString("\n")
      SqlDataSourceConfig.dataSourcesFromString(jsonString) shouldEqual Map(
        ("CENSUS_ORACLE", SqlDataSourceConfig("jdbc", "CENSUS_ORACLE", None, Some("jdbc:h2:mem:CENSUS.db;INIT=CREATE SCHEMA IF NOT EXISTS CENSUS;DB_CLOSE_DELAY=30;"), Some("org.h2.Driver"), 100)),
        ("ORACLE_X2", SqlDataSourceConfig("jdbc", "ORACLE_X2", None, Some("jdbc:h2:mem:X2.db;INIT=CREATE SCHEMA IF NOT EXISTS X2;DB_CLOSE_DELAY=30;"), Some("org.h2.Driver"), 10)),
        ("HIVE_CENSUS", SqlDataSourceConfig("hive", "HIVE_CENSUS", None, None, None, 100)),
        ("HIVE_X2", SqlDataSourceConfig("hive", "HIVE_X2", None, None, None, 100))
      )
    }
  }

}
