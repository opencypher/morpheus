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

import org.opencypher.spark.api.io.{HiveFormat, JdbcFormat, StorageFormat, TabularFileFormat}
import ujson.Value

import scala.util.{Failure, Success, Try}

case class SqlDataSourceConfigException(msg: String, cause: Throwable = null) extends Throwable(msg, cause)


import org.opencypher.okapi.impl.util.JsonUtils.FlatOption._

sealed abstract class SqlDataSourceConfig(
  val format: StorageFormat,
  val options: Map[String, String]
)

object SqlDataSourceConfig {
  private implicit val jdbc: ReadWriter[Jdbc] = macroRW
  private implicit val hive: ReadWriter[Hive] = macroRW
  private implicit val file: ReadWriter[File] = macroRW
  private val defaultMacroRW: ReadWriter[SqlDataSourceConfig] = macroRW

  private final val UJSON_TYPE_KEY = "$type"
  private final val CAPS_TYPE_KEY = "type"

  implicit val rw: ReadWriter[SqlDataSourceConfig] = readwriter[Value].bimap[SqlDataSourceConfig](
    // Rename discriminator key from ujson default, to a more friendly version
    cfg => writeJs(cfg)(defaultMacroRW).obj.collect {
      case (UJSON_TYPE_KEY, value) => CAPS_TYPE_KEY -> value
      case other => other
    },
    // Revert name change so we can use the ujson reader
    js => read[SqlDataSourceConfig](js.obj.map {
      case (CAPS_TYPE_KEY, value) => UJSON_TYPE_KEY -> value
      case other => other
    })(defaultMacroRW)
  )

  def toJson(dataSource: SqlDataSourceConfig, indent: Int = 4): String =
    write[SqlDataSourceConfig](dataSource, indent)

  def fromJson(jsonString: String): SqlDataSourceConfig =
    read[SqlDataSourceConfig](jsonString)

  def dataSourcesFromString(jsonStr: String): Map[String, SqlDataSourceConfig] =
    Try(read[Map[String, SqlDataSourceConfig]](jsonStr)) match {
      case Success(result) => result
      case Failure(ex) =>
        throw SqlDataSourceConfigException(s"Malformed SQL configuration file: ${ex.getMessage}", ex)
    }

//  * @param storageFormat  the interface between the SQL PGDS and the SQL data source. Supported values are hive and jdbc.
//  * @param dataSourceName the user-defined name of the data source.
//  * @param defaultSchema  the default SQL schema to use in this data source. Can be overridden by SET SCHEMA in Graph DDL.
//    * @param jdbcUri
//  * @param jdbcDriver     classname of the JDBC driver to use for the JDBC connection
//  * @param jdbcFetchSize  the fetch size to use for transferring data over JDBC
//    * @param basePath       the root folder used for file based formats

  /** Configures a data source that reads tables via JDBC
    *
    * @param url     the JDBC URI to use when connecting to the JDBC server
    * @param driver  class name of the JDBC driver to use for the JDBC connection
    * @param options extra options passed to Spark when configuring the reader
    */
  @upickle.implicits.key("jdbc")
  case class Jdbc(
    url: String,
    driver: String,
    override val options: Map[String, String] = Map.empty
  ) extends SqlDataSourceConfig(JdbcFormat, options)

  /** Configures a data source that reads tables from Hive
    * @note The Spark session needs to be configured with `.enableHiveSupport()`
    */
  @upickle.implicits.key("hive")
  case class Hive(
  ) extends SqlDataSourceConfig(HiveFormat, Map.empty)

  /** Configures a data source that reads tables from files
    *
    * @param format   the file format passed to Spark when configuring the reader
    * @param basePath the root folder used for file based formats
    * @param options  extra options passed to Spark when configuring the reader
    */
  @upickle.implicits.key("file")
  case class File(
    override val format: TabularFileFormat,
    basePath: Option[String] = None,
    override val options: Map[String, String] = Map.empty
  ) extends SqlDataSourceConfig(format, options)

}
