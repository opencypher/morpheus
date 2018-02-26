/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
 */
package org.opencypher.okapi.api.schema

import cats.instances.all._
import cats.syntax.semigroup._
import org.opencypher.okapi.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.api.types.CypherType.joinMonoid

object RelTypePropertyMap {
  val empty: RelTypePropertyMap = RelTypePropertyMap(Map.empty)

  /**
    * Sets all cypher types of properties that are not common across all labels to nullable.
    *
    * @param map property key map
    * @return updated property key map
    */
  def asNullable(map: RelTypePropertyMap): RelTypePropertyMap = {
    val overlap = map.map.map(_._2.keySet).reduce(_ intersect _)

    RelTypePropertyMap(map.map.map { pair =>
      pair._1 -> pair._2.map(p2 => p2._1 -> (if (overlap.contains(p2._1)) p2._2 else p2._2.nullable))
    })
  }
}

final case class RelTypePropertyMap(map: Map[String, PropertyKeys]) {

  def register(relType: String, keys: PropertyKeys): RelTypePropertyMap = {
    val oldKeys = map.getOrElse(relType, Map.empty)
    copy(map.updated(relType, oldKeys ++ keys))
  }

  def properties(relKey: String): PropertyKeys = map.getOrElse(relKey, Map.empty)

  def filterForRelTypes(relType: Set[String]): RelTypePropertyMap = {
    RelTypePropertyMap(map.filterKeys(relType.contains))
  }

  def ++(other: RelTypePropertyMap): RelTypePropertyMap = copy(map |+| other.map)

  // utility signatures

  def register(relType: String)(keys: (String, CypherType)*): RelTypePropertyMap =
    register(relType, keys.toMap)

  def register(relType: String, keys: => Seq[(String, CypherType)]): RelTypePropertyMap =
    register(relType, keys.toMap)
}
