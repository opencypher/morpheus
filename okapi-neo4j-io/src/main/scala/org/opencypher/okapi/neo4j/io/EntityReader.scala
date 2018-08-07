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
package org.opencypher.okapi.neo4j.io

import org.opencypher.okapi.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.neo4j.io.Neo4jHelpers.Neo4jDefaults._

object EntityReader {

  def flatExactLabelQuery(labels: Set[String], schema: Schema, maybeMetaLabel: Option[String] = None): String ={
    val props = schema.nodeKeys(labels).propertyExtractorString
    val allLabels = labels ++ maybeMetaLabel
    val labelCount = allLabels.size

    s"""|MATCH ($entityVarName:${allLabels.mkString(":")})
        |WHERE length(labels($entityVarName)) = $labelCount
        |RETURN id($entityVarName) AS $idPropertyKey$props""".stripMargin
  }

  def flatRelTypeQuery(relType: String, schema: Schema, maybeMetaLabel: Option[String] = None): String ={
    val props = schema.relationshipKeys(relType).propertyExtractorString
    val metaLabel = maybeMetaLabel.map(m => s":$m").getOrElse("")

    s"""|MATCH (s$metaLabel)-[$entityVarName:$relType]->(t$metaLabel)
        |RETURN id($entityVarName) AS $idPropertyKey, id(s) AS $startIdPropertyKey, id(t) AS $endIdPropertyKey$props""".stripMargin
  }

  implicit class RichPropertyTypes(val properties: PropertyKeys) extends AnyVal {
    def propertyExtractorString: String = {
      val propertyStrings = properties
        .keys
        .toList
        .sorted
        .map(k => s"$entityVarName.$k")

      if (propertyStrings.isEmpty) ""
      else propertyStrings.mkString(", ", ", ", "")
    }
  }
}
