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
package org.opencypher.okapi.ir

import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.ir.api.{IRField, IRCatalogGraph}

import scala.language.implicitConversions

package object test {
  implicit def toVar(s: Symbol): Var = Var(s.name)()

  implicit def toVar(t: (Symbol, CypherType)): Var = Var(t._1.name)(t._2)

  implicit def toField(s: Symbol): IRField = IRField(s.name)()

  implicit def toField(t: (Symbol, CypherType)): IRField = IRField(t._1.name)(t._2)

  implicit def toGraph(s: Symbol): IRCatalogGraph = IRCatalogGraph(s.name, Schema.empty)
}
