/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.caps.api.schema

import org.opencypher.caps.api.types.CypherType

object PropertyKeyMap {
  val empty: PropertyKeyMap = PropertyKeyMap(Map.empty)()

  /**
    * Sets all cypher types of properties that are not common across all labels to nullable.
    *
    * @param map property key map
    * @return updated property key map
    */
  def asNullable(map: PropertyKeyMap): PropertyKeyMap = {
    val overlap = map.m.map(_._2.keySet).reduce(_ intersect _)

    PropertyKeyMap(map.m.map {
      pair => pair._1 -> pair._2.map(p2 => p2._1 -> (if (overlap.contains(p2._1)) p2._2 else p2._2.nullable))
    })()
  }
}

final case class PropertyKeyMap(m: Map[String, Map[String, CypherType]])(val conflicts: Set[String] = Set.empty) {
  def keysFor(classifier: String): Map[String, CypherType] = m.getOrElse(classifier, Map.empty)
  def withKeys(classifier: String, keys: Seq[(String, CypherType)]): PropertyKeyMap = {
    val oldKeys = m.getOrElse(classifier, Map.empty)
    val newKeys = keys.toMap
    val newConflicts = oldKeys.collect {
      case (k, t) =>
        newKeys.get(k) match {
          case Some(otherT) if t != otherT =>
            Some(s"Conflicting schema for '$classifier'! Key '$k' has type $t but also has type ${newKeys(k)}")
          case _ =>
            None
        }
    }.flatten.toSet
    copy(m.updated(classifier, oldKeys ++ newKeys))(conflicts = conflicts ++ newConflicts)
  }

  def keys: Set[String] = m.values.flatMap(_.keySet).toSet

  def ++(other: PropertyKeyMap): PropertyKeyMap = {
    val joined = joinMaps(m, other.m)((leftAttr, rightAttr) => joinMaps(leftAttr, rightAttr)(_ join _, _.nullable))
    copy(joined)(conflicts ++ other.conflicts)
  }

  private def joinMaps[A, B](left: Map[A, B], right: Map[A, B])
                            (joinF: (B, B) => B, mapF: B => B = (x: B) => x): Map[A, B] = {
    val uniqueLeft = left.keySet -- right.keySet
    val withUniqueLeft = uniqueLeft.foldLeft(Map[A, B]())((map, key) => map.updated(key, mapF(left(key))))

    val uniqueRight = right.keySet -- left.keySet
    val withUniqueRight = uniqueRight.foldLeft(withUniqueLeft)((map, key) => map.updated(key, mapF(right(key))))

    val common = left.keySet.intersect(right.keySet)
    common.foldLeft(withUniqueRight) {(map, key) => map.updated(key, joinF(left(key), right(key)))}
  }
}
