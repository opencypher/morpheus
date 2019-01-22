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
package org.opencypher.spark.api.io.fs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.opencypher.spark.impl.table.SparkTable._

trait EscapeAtSymbol extends FSGraphSource {

  private val unicodeEscaping = "_specialCharacterEscape_"
  private val atSymbol = "@"

  abstract override def writeTable(path: String, table: DataFrame): Unit = {
    schemaCheck(table.schema)
    val writeMapping = encodedColumnNames(table.schema)
    val newTable = table.safeRenameColumns(writeMapping: _*)
    super.writeTable(path, newTable)
  }

  abstract override def readTable(path: String, schema: StructType): DataFrame = {
    val readMapping = encodedColumnNames(schema).toMap
    val readSchema = StructType(schema.fields.map { f =>
      f.copy(name = readMapping.getOrElse(f.name, f.name))
    })

    val outMapping = readMapping.map(_.swap).toSeq
    super.readTable(path, readSchema).safeRenameColumns(outMapping: _*)
  }

  private def encodedColumnNames(schema: StructType): Seq[(String, String)] = {
    schema.fields
      .map(f => f.name -> f.name.replaceAll(atSymbol, unicodeEscaping))
      .filterNot { case (from, to) => from == to}
  }

  private def schemaCheck(schema: StructType): Unit = {
    val invalidFields = schema.fields.filter(f => f.name.contains(unicodeEscaping)).map(_.name)
    if (invalidFields.nonEmpty) sys.error(s"Orc fields: $invalidFields cannot contain special encoding string: '$unicodeEscaping'")
  }

}
