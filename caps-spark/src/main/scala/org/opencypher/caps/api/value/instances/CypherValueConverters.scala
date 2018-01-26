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
package org.opencypher.caps.api.value.instances

import org.opencypher.caps.api.types.Ternary
import org.opencypher.caps.api.value._
import org.opencypher.caps.api.value.syntax.cypherNull

import scala.language.implicitConversions

trait CypherValueConverters extends LowPriorityCypherValueConverters {

  implicit def cypherBoolean(v: Boolean): CAPSBoolean = CAPSBoolean.create(v)
  implicit def cypherString(v: String): CAPSString = CAPSString.create(v)
  implicit def cypherInteger(v: Byte): CAPSInteger = CAPSInteger.create(v.toLong)
  implicit def cypherInteger(v: Short): CAPSInteger = CAPSInteger.create(v.toLong)
  implicit def cypherInteger(v: Int): CAPSInteger = CAPSInteger.create(v.toLong)
  implicit def cypherInteger(v: Long): CAPSInteger = CAPSInteger.create(v)
  implicit def cypherFloat(v: Float): CAPSFloat = CAPSFloat.create(v.toDouble)
  implicit def cypherFloat(v: Double): CAPSFloat = CAPSFloat.create(v)

  implicit def cypherTernary(v: Ternary): CAPSValue =
    if (v.isDefinite) CAPSBoolean.create(v.isTrue) else cypherNull[CAPSValue]

  implicit def cypherOption[T](v: Option[T])(implicit ev: T => CAPSValue): CAPSValue =
    v.map(ev).getOrElse(cypherNull[CAPSValue])

  implicit def cypherList[T](v: Seq[T])(implicit ev: T => CAPSValue): CAPSList =
    CAPSList.create(v.map(ev))

  implicit def cypherMap(v: MapContents): CAPSMap =
    CAPSMap.create(v)

  implicit def cypherMap(v: Properties): CAPSMap =
    CAPSMap.create(v)

  implicit def cypherMap[K, V](v: Map[K, V])(implicit ev: V => CAPSValue): CAPSMap = {
    val contents = Properties.fromMap(v.map(entry => entry._1.toString -> ev(entry._2)))
    CAPSMap.create(contents)
  }

  implicit def cypherNode(v: NodeContents): CAPSNode =
    CAPSNode.create(v)

  implicit def cypherRelationship(v: RelationshipContents): CAPSRelationship =
    CAPSRelationship.create(v)
}
