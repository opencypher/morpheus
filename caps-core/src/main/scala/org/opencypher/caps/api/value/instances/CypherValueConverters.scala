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
package org.opencypher.caps.api.value.instances

import org.opencypher.caps.api.value._
import org.opencypher.caps.api.value.syntax.cypherNull
import org.opencypher.caps.common.Ternary

import scala.language.implicitConversions

trait CypherValueConverters
  extends LowPriorityCypherValueConverters {

  implicit def cypherBoolean(v: Boolean): CypherBoolean = CypherBoolean.create(v)
  implicit def cypherString(v: String): CypherString = CypherString.create(v)
  implicit def cypherInteger(v: Byte): CypherInteger = CypherInteger.create(v.toLong)
  implicit def cypherInteger(v: Short): CypherInteger = CypherInteger.create(v.toLong)
  implicit def cypherInteger(v: Int): CypherInteger = CypherInteger.create(v.toLong)
  implicit def cypherInteger(v: Long): CypherInteger = CypherInteger.create(v)
  implicit def cypherFloat(v: Float): CypherFloat =  CypherFloat.create(v.toDouble)
  implicit def cypherFloat(v: Double): CypherFloat = CypherFloat.create(v)

  implicit def cypherTernary(v: Ternary): CypherValue =
    if (v.isDefinite) CypherBoolean.create(v.isTrue) else cypherNull[CypherValue]

  implicit def cypherOption[T](v: Option[T])(implicit ev: T => CypherValue): CypherValue =
    v.map(ev).getOrElse(cypherNull[CypherValue])

  implicit def cypherList[T](v: Seq[T])(implicit ev: T => CypherValue): CypherList =
    CypherList.create(v.map(ev))

  implicit def cypherMap(v: MapContents): CypherMap =
    CypherMap.create(v)

  implicit def cypherMap(v: Properties): CypherMap =
    CypherMap.create(v)

  implicit def cypherMap[K, V](v: Map[K, V])(implicit ev: V => CypherValue): CypherMap = {
    val contents = Properties.fromMap(v.map(entry => entry._1.toString -> ev(entry._2)))
    CypherMap.create(contents)
  }

  implicit def cypherNode(v: NodeContents): CypherNode =
    CypherNode.create(v)

  implicit def cypherRelationship(v: RelationshipContents): CypherRelationship =
    CypherRelationship.create(v)
}
