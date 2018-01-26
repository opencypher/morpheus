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
package org.opencypher.caps.api.value

import org.opencypher.caps.api.types._

trait CypherValue {
  def cypherType: CypherType
}

trait CypherList extends CypherValue {
  override def cypherType: CTList
}

trait CypherBoolean extends CypherValue {
  override def cypherType = CTBoolean
}

trait CypherInteger extends CypherValue {
  def value: Long

  override def cypherType = CTInteger
}

trait CypherFloat extends CypherValue {
  def value: Double

  override def cypherType = CTFloat
}

trait CypherString extends CypherValue {
  def value: String

  override def cypherType = CTString
}

trait CypherMap extends CypherValue {
  def get(key: String): Option[CypherValue]

  def keys: Set[String]

  override def cypherType = CTMap
}

trait CypherPath extends CypherValue {

  override def cypherType = CTPath
}
