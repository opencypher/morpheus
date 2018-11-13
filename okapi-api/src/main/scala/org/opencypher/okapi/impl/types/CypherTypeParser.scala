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
package org.opencypher.okapi.impl.types

import fastparse.core.Parsed.{Failure, Success}
import org.apache.logging.log4j.scala.Logging
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.util.ParserUtils._

//noinspection ForwardReference
object CypherTypeParser extends Logging {

  def parse(input: String): Option[CypherType] = {
    cypherType.entireInput.parse(input) match {
      case Success(value, _) => Some(value)
      case Failure(_, index, extra) =>
        logger.debug(s"Failed to parse $input at index $index: ${extra.traced.trace}")
        None
    }
  }

  import fastparse.noApi._
  import org.opencypher.okapi.impl.util.ParserUtils.Whitespace._
  import org.opencypher.okapi.impl.util.ParserUtils._

  // Basic types
  val STRING: P[CTString.type] = IgnoreCase("STRING").map(_ => CTString)
  val INTEGER: P[CTInteger.type] = IgnoreCase("INTEGER").map(_ => CTInteger)
  val FLOAT: P[CTFloat.type] = IgnoreCase("FLOAT").map(_ => CTFloat)
  val NUMBER: P[CTNumber.type] = IgnoreCase("NUMBER").map(_ => CTNumber)
  val BOOLEAN: P[CTBoolean.type] = IgnoreCase("BOOLEAN").map(_ => CTBoolean)
  val ANY: P[CTAny.type] = IgnoreCase("ANY").map(_ => CTAny)
  val VOID: P[CTVoid.type] = IgnoreCase("VOID").map(_ => CTVoid)
  val NULL: P[CTNull.type] = IgnoreCase("NULL").map(_ => CTNull)
  val WILDCARD: P[CTWildcard.type] = IgnoreCase("?").map(_ => CTWildcard)

  // element types
  val NODE: P[CTNode] = P(
    IgnoreCase("NODE") ~ ("(" ~ label.rep() ~ ")").?
  ).map(l => CTNode(l.getOrElse(Seq.empty).toSet))

  val RELATIONSHIP: P[CTRelationship] = P(
    IgnoreCase("RELATIONSHIP") ~ ("(" ~ label.rep(sep = "|") ~ ")").?
  ).map(l => CTRelationship(l.getOrElse(Seq.empty).toSet))

  val PATH: P[CTPath.type] = IgnoreCase("PATH").map(_ => CTPath)

  // container types
  val LIST: P[CTList] = P(IgnoreCase("LIST") ~ "(" ~ cypherType ~ ")").map(inner => CTList(inner))

  private val mapKey: P[String] = identifier.! | escapedIdentifier
  private val kvPair: P[(String, CypherType)] = P(mapKey ~ ":" ~ cypherType)
  val MAP: P[CTMap] = P(IgnoreCase("MAP") ~ "(" ~ kvPair.rep(sep = ",") ~ ")").map(inner => CTMap(inner.toMap))

  val materialCypherType: P[CypherType] = P(
    STRING |
    INTEGER |
    FLOAT |
    NUMBER |
    BOOLEAN |
    ANY |
    VOID |
    NULL |
    WILDCARD |
    NODE |
    RELATIONSHIP |
    PATH |
    LIST |
    MAP
  )

  val nullableCypherType: P[CypherType] = P(materialCypherType ~ "?").map(ct => ct.nullable)

  val cypherType: P[CypherType] = nullableCypherType | materialCypherType
}
