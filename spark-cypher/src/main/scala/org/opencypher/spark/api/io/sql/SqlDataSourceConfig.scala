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

import org.opencypher.okapi.impl.util.JsonUtils.FlatOption._
import org.opencypher.spark.api.io.StorageFormat

import scala.util.{Failure, Success, Try}

object SqlDataSourceConfig {

  implicit def rw: ReadWriter[SqlDataSourceConfig] = macroRW

  def toJson(dataSource: SqlDataSourceConfig): String = write[SqlDataSourceConfig](dataSource, indent = 4)

  def fromJson(jsonString: String): SqlDataSourceConfig = read[SqlDataSourceConfig](jsonString)

  def dataSourcesFromString(jsonStr: String): Map[String, SqlDataSourceConfig] = {
    val json = Try(read[List[SqlDataSourceConfig]](jsonStr)) match {
      case Success(result) => result
      case Failure(ex) =>
        throw SqlDataSourceConfigException(s"Malformed SQL configuration file: ${ex.getMessage}", ex)
    }
    json.map(data => data.dataSourceName -> data).toMap
  }
}

/**
  * Configuration for a SQL data source from which the SQL PGDS can extract data.
  *
  * @param storageFormat  the interface between the SQL PGDS and the SQL data source. Supported values are hive and jdbc.
  * @param dataSourceName the user-defined name of the data source.
  * @param defaultSchema  the default SQL schema to use in this data source. Can be overridden by SET SCHEMA in Graph DDL.
  * @param jdbcUri        the JDBC URI to use when connecting to the JDBC server
  * @param jdbcDriver     classname of the JDBC driver to use for the JDBC connection
  * @param jdbcFetchSize  the fetch size to use for transferring data over JDBC
  * @param basePath       the root folder used for file based formats
  */
case class SqlDataSourceConfig(
  storageFormat: StorageFormat,
  dataSourceName: String,
  defaultSchema: Option[String] = None,
  jdbcUri: Option[String] = None,
  jdbcDriver: Option[String] = None,
  jdbcUser: Option[String] = None,
  jdbcPassword: Option[String] = None,
  jdbcFetchSize: Int = 100,
  basePath: Option[String] = None
) {
  def toJson: String = SqlDataSourceConfig.toJson(this)
}

case class SqlDataSourceConfigException(msg: String, cause: Throwable = null) extends Throwable(msg, cause)
