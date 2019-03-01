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
package org.opencypher.okapi.ir.impl.typer

import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.api.types.CypherType._
import org.opencypher.okapi.api.value.CypherValue.CypherValue
import org.neo4j.cypher.internal.v4_0.expressions.Expression

import scala.annotation.tailrec

object TypeTracker {
  val empty = TypeTracker(Map.empty)
}

case class TypeTracker(map: Map[Expression, CypherType], parameters: Map[String, CypherValue] = Map.empty) {

  def withParameters(newParameters: Map[String, CypherValue]): TypeTracker = copy(parameters = newParameters)

  def get(e: Expression): Option[CypherType] = map.get(e)

  def getParameterType(e: String): Option[CypherType] = parameters.get(e).map(_.cypherType)

  def updated(e: Expression, t: CypherType): TypeTracker = copy(map = map.updated(e, t))
}
