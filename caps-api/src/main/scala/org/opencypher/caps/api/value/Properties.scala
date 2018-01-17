/*
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
package org.opencypher.caps.api.value

import org.opencypher.caps.api.exception.IllegalArgumentException
import org.opencypher.caps.api.value.syntax.cypherNull

import scala.collection.immutable.SortedMap
import scala.language.implicitConversions

object Properties {

  val empty = new Properties(SortedMap.empty)

  def apply(elts: (String, CypherValue)*): Properties =
    fromMap(SortedMap(elts: _*))

  implicit def fromMap(v: Map[String, CypherValue]): Properties = {
    if (v == null) throw IllegalArgumentException("a property map", v)
    v match {
      case m: SortedMap[String, CypherValue] if m.ordering eq Ordering.String =>
        new Properties(m)

      case _ =>
        new Properties(SortedMap(v.toSeq: _*)(Ordering.String))
    }
  }
}

final class Properties private (val m: SortedMap[String, CypherValue]) extends AnyVal with Serializable {

  def isEmpty: Boolean = m.isEmpty

  def apply(key: String): CypherValue = m.getOrElse(key, cypherNull[CypherValue])
  def get(key: String): Option[CypherValue] = m.get(key)

  def containsNullValue: Boolean = m.values.exists(CypherValue.isOrContainsNull)
}
