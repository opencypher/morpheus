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
package org.opencypher.okapi.api.io.conversion

import org.opencypher.okapi.api.graph.{PatternElement, IdKey, Pattern}
import org.opencypher.okapi.api.types.CTRelationship
import org.opencypher.okapi.impl.exception.IllegalArgumentException

object ElementMapping {
  def empty(pattern: Pattern) = ElementMapping(pattern, Map.empty, Map.empty)
}

/**
  * Represents a mapping from a source with key-based access of element components (e.g. a table definition) to a Pattern.
  * The purpose of this class is to define a mapping from an external data source to a property graph.
  *
  * The [[pattern]] describes the shape of the pattern that is described by this mapping
  *
  * The [[idKeys]] describe the mappings for each pattern element, which map the element identifiers
  * to columns within the source data.
  *
  * The [[properties]] represent mappings for every pattern element from property keys to keys in the source data.
  * The retrieved value from the source is expected to be convertible to a valid [[org.opencypher.okapi.api.value.CypherValue]].
  *
  * @param pattern    the pattern described by this mapping
  * @param properties mapping from property key to source property key
  * @param idKeys     mapping for the key to access the element identifier in the source data
  */
case class ElementMapping(
  pattern: Pattern,
  properties: Map[PatternElement, Map[String, String]],
  idKeys: Map[PatternElement, Map[IdKey, String]]
) {

  validate()

  lazy val allSourceIdKeys: Seq[String] = idKeys.values.flatMap(keyMapping => keyMapping.values).toSeq.sorted

  lazy val allSourcePropertyKeys: Seq[String] = properties.values.flatMap(keyMapping => keyMapping.values).toSeq.sorted

  lazy val allSourceKeys: Seq[String] = (allSourceIdKeys ++ allSourcePropertyKeys).sorted

  protected def validate(): Unit = {
    val sourceKeys = allSourceKeys
    if (allSourceKeys.size != sourceKeys.toSet.size) {
      val duplicateColumns = sourceKeys.groupBy(identity).filter { case (_, items) => items.size > 1 }
      throw IllegalArgumentException(
        "One-to-one mapping from element elements to source keys",
        s"Duplicate columns: $duplicateColumns")
    }

    pattern.elements.foreach {
      case e@PatternElement(_, CTRelationship(types, _)) if types.size != 1 =>
        throw IllegalArgumentException(
          s"A single implied type for element $e",
          types
        )
      case _ => ()
    }
  }
}

