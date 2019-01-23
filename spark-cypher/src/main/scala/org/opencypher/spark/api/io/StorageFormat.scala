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
package org.opencypher.spark.api.io

import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.impl.util.JsonUtils.FlatOption._
import org.opencypher.spark.api.io.StorageFormat.nonFileFormatNames
import ujson._

trait StorageFormat {
  def name: String = getClass.getSimpleName.dropRight("Format$".length).toLowerCase
}

object StorageFormat {

  val nonFileFormats: Map[String, StorageFormat] = Map(
    AvroFormat.name -> AvroFormat,
    HiveFormat.name -> HiveFormat,
    JdbcFormat.name -> JdbcFormat,
    Neo4jFormat.name -> Neo4jFormat
  )

  val nonFileFormatNames: Set[String] = nonFileFormats.keySet

  private def unexpected(name: String, available: Iterable[String]) =
    throw IllegalArgumentException(s"Supported storage format (one of ${available.mkString("[", ", ", "]")})", name)

  implicit def rwStorageFormat: ReadWriter[StorageFormat] = readwriter[Value].bimap[StorageFormat](
    (storageFormat: StorageFormat) => storageFormat.name,
    (storageFormatName: Value) => {
      val formatString = storageFormatName.str
      nonFileFormats.getOrElse(formatString, FileFormat(formatString))
    }
  )

  implicit def rwFileFormat: ReadWriter[FileFormat] = readwriter[Value].bimap[FileFormat](
    (fileFormat: FileFormat) => fileFormat.name,
    (fileFormatName: Value) => FileFormat(fileFormatName.str)
  )

}

case object AvroFormat extends StorageFormat

case object Neo4jFormat extends StorageFormat

case object HiveFormat extends StorageFormat

case object JdbcFormat extends StorageFormat

object FileFormat {
  val csv: FileFormat = FileFormat("csv")
  val orc: FileFormat = FileFormat("orc")
  val parquet: FileFormat = FileFormat("parquet")
}

case class FileFormat(override val name: String) extends StorageFormat {
  assert(!nonFileFormatNames.contains(name),
    s"Cannot create a file format with a name in ${nonFileFormatNames.mkString("[", ", ", "]")} ")
}
