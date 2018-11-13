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
package org.opencypher.okapi.ir.impl.typer

import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.api.types.CypherType._
import org.opencypher.okapi.api.value.CypherValue.CypherValue
import org.opencypher.v9_0.expressions.Expression

import scala.annotation.tailrec

object TypeTracker {
  val empty = TypeTracker(List.empty)
}

case class TypeTracker(maps: List[Map[Expression, CypherType]], parameters: Map[String, CypherValue] = Map.empty) {

  def withParameters(newParameters: Map[String, CypherValue]): TypeTracker =
    copy(parameters = newParameters)

  def get(e: Expression): Option[CypherType] = get(e, maps)

  def getParameterType(e: String): Option[CypherType] = parameters.get(e).map(_.cypherType)

  @tailrec
  private def get(e: Expression, maps: List[Map[Expression, CypherType]]): Option[CypherType] = maps.headOption match {
    case None                         => None
    case Some(map) if map.contains(e) => map.get(e)
    case Some(_)                      => get(e, maps.tail)
  }

  def updated(e: Expression, t: CypherType): TypeTracker = copy(maps = head.updated(e, t) +: tail)
  def updated(entry: (Expression, CypherType)): TypeTracker = updated(entry._1, entry._2)
  def pushScope(): TypeTracker = copy(maps = Map.empty[Expression, CypherType] +: maps)
  def popScope(): Option[TypeTracker] = if (maps.isEmpty) None else Some(copy(maps = maps.tail))

  private def head: Map[Expression, CypherType] =
    maps.headOption.getOrElse(Map.empty[Expression, CypherType])
  private def tail: List[Map[Expression, CypherType]] =
    if (maps.isEmpty) List.empty else maps.tail
}
