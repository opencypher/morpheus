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

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.junit.rules.TemporaryFolder
import org.opencypher.spark.api.io.sql.IdGenerationStrategy.{IdGenerationStrategy, _}
import org.opencypher.spark.api.io.{OrcFormat, ParquetFormat, StorageFormat}

abstract class FileSqlPGDSAcceptanceTest extends SqlPropertyGraphDataSourceAcceptanceTest {

  def format: StorageFormat

  var tempDir: TemporaryFolder = new TemporaryFolder()
  lazy val basePath: String =  "file:///" + tempDir.getRoot.getAbsolutePath.replace("\\", "/")

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir.create()
  }

  override def afterAll(): Unit = {
    tempDir.delete()
    super.afterAll()
  }

  override def sqlDataSourceConfig: SqlDataSourceConfig =
    SqlDataSourceConfig(format, dataSourceName, basePath = Some(basePath))

  override def writeTable(df: DataFrame, tableName: String): Unit = {
    val path = basePath + s"/${tableName.replace(s"$databaseName.","")}"
    df.write.mode(SaveMode.Overwrite).format(format.name).save(path)
  }
}

class ParquetSqlPGDSAcceptanceMonotonicallyIncreasingIdTest extends FileSqlPGDSAcceptanceTest {
  override def format: StorageFormat = ParquetFormat
  override val idGenerationStrategy: IdGenerationStrategy = MonotonicallyIncreasingId
}

class ParquetSqlPGDSAcceptanceHashBasedTest extends FileSqlPGDSAcceptanceTest {
  override def format: StorageFormat = ParquetFormat
  override val idGenerationStrategy: IdGenerationStrategy = HashBasedId
}


class OrcSqlPGDSAcceptanceMonotonicallyIncreasingIdTest extends FileSqlPGDSAcceptanceTest {
  override def format: StorageFormat = OrcFormat
  override val idGenerationStrategy: IdGenerationStrategy = MonotonicallyIncreasingId
}

class OrcSqlPGDSAcceptanceHashBasedTest extends FileSqlPGDSAcceptanceTest {
  override def format: StorageFormat = OrcFormat
  override val idGenerationStrategy: IdGenerationStrategy = HashBasedId
}
