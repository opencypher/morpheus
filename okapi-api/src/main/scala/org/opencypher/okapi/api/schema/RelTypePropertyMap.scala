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
package org.opencypher.okapi.api.schema

import cats.instances.all._
import cats.syntax.semigroup._
import org.opencypher.okapi.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.okapi.api.types.{CTNode, CTRelationship, CypherType}
import org.opencypher.okapi.api.types.CypherType.joinMonoid

object RelTypePropertyMap {

  type RelTypePropertyMap = Map[String, PropertyKeys]

  val empty: RelTypePropertyMap = Map.empty

  implicit class RichRelTypePropertyMap(val map: Map[String, PropertyKeys]) extends AnyVal {

    def register(relType: String, keys: PropertyKeys): RelTypePropertyMap = {
      val oldKeys = map.getOrElse(relType, Map.empty)
      map.updated(relType, oldKeys ++ keys)
    }

    def properties(relKey: String): PropertyKeys = map.getOrElse(relKey, Map.empty)

    def cypherType(relKey: String): Option[CTRelationship] =
      map.get(relKey).map(CTRelationship(Set(relKey), _))

    def filterForRelTypes(relType: Set[String]): RelTypePropertyMap = map.filterKeys(relType.contains)

    def ++(other: RelTypePropertyMap): RelTypePropertyMap = map |+| other

    // utility signatures

    def register(relType: String)(keys: (String, CypherType)*): RelTypePropertyMap = register(relType, keys.toMap)

    def register(relType: String, keys: => Seq[(String, CypherType)]): RelTypePropertyMap = register(relType, keys.toMap)

    /**
      * Sets all cypher types of properties that are not common across all labels to nullable.
      *
      * @return updated property key map
      */
    def asNullable: RelTypePropertyMap = {
      val overlap = map.map(_._2.keySet).reduce(_ intersect _)

      map.map { pair =>
        pair._1 -> pair._2.map(p2 => p2._1 -> (if (overlap.contains(p2._1)) p2._2 else p2._2.nullable))
      }
    }

  }

}
