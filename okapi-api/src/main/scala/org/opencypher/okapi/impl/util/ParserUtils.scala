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
package org.opencypher.okapi.impl.util

import fastparse._
import NoWhitespace._

object ParserUtils {
  def newline[_: P]: P[Unit] = P("\n" | "\r\n" | "\r" | "\f")
  def invisible[_: P]: P[Unit] = P(" " | "\t" | newline)
  def comment[_: P]: P[Unit] = P("--" ~ (!newline ~ AnyChar).rep ~ (newline | &(End)))
  implicit val whitespace: P[_] => P[Unit] = { implicit ctx: ParsingRun[_] => (comment | invisible).rep }

  def keyword[_: P](k: String): P[Unit] = P(IgnoreCase(k))
  def digit[_: P]: P[Unit] = P(CharIn("0-9"))
  def character[_: P]: P[Unit] = P(CharIn("a-zA-Z"))
  def identifier[_: P]: P[Unit] = P(character ~~ P(character | digit | "_").repX)
  def escapedIdentifier[_: P]: P[String] = P(identifier.! | ("`" ~~ CharsWhile(_ != '`').! ~~ "`"))
  def label[_: P]: P[String] = P(":" ~ (identifier.! | escapedIdentifier))
}
