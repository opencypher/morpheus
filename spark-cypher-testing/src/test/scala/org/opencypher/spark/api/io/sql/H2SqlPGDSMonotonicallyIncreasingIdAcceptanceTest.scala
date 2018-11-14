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

import org.apache.spark.sql.DataFrame
import org.opencypher.spark.api.io.JdbcFormat
import org.opencypher.spark.api.io.sql.IdGenerationStrategy.{IdGenerationStrategy, _}
import org.opencypher.spark.testing.fixture.H2Fixture
import org.opencypher.spark.testing.utils.H2Utils._

class H2SqlPGDSMonotonicallyIncreasingIdAcceptanceTest extends SqlPropertyGraphDataSourceAcceptanceTest with H2Fixture {

  override def idGenerationStrategy: IdGenerationStrategy = MonotonicallyIncreasingId

  override def beforeAll(): Unit = {
    super.beforeAll()
    createH2Database(sqlDataSourceConfig, databaseName)
  }

  override def afterAll(): Unit = {
    dropH2Database(sqlDataSourceConfig, databaseName)
    super.afterAll()
  }

  override def sqlDataSourceConfig: SqlDataSourceConfig =
    SqlDataSourceConfig(
      storageFormat = JdbcFormat,
      dataSourceName = dataSourceName,
      jdbcDriver = Some("org.h2.Driver"),
      jdbcUri = Some("jdbc:h2:mem:?user=sa&password=1234;DB_CLOSE_DELAY=-1")
    )

  override def writeTable(df: DataFrame, tableName: String): Unit =
    df.saveAsSqlTable(sqlDataSourceConfig, tableName)

}
