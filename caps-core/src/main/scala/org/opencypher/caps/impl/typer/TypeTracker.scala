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
package org.opencypher.caps.impl.typer

import org.neo4j.cypher.internal.frontend.v3_3.ast.Expression
import org.opencypher.caps.api.types.CypherType

import scala.annotation.tailrec

object TypeTracker {
  val empty = TypeTracker(List.empty)
}

case class TypeTracker(maps: List[Map[Expression, CypherType]], parameters: Map[String, CypherType] = Map.empty) {

  def withParameters(newParameters: Map[String, CypherType]): TypeTracker =
    copy(parameters = newParameters)

  def get(e: Expression): Option[CypherType] = get(e, maps)

  def getParameter(e: String): Option[CypherType] = parameters.get(e)

  @tailrec
  private def get(e: Expression, maps: List[Map[Expression, CypherType]]): Option[CypherType] = maps.headOption match {
    case None => None
    case Some(map) if map.contains(e) => map.get(e)
    case Some(_) => get(e, maps.tail)
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
