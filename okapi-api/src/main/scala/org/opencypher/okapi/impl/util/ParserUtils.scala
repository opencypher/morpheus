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
package org.opencypher.okapi.impl.util

import fastparse.WhitespaceApi

object ParserUtils {
  object ParsersForNoTrace {

    import fastparse.all._

    val newline: P[Unit] = P("\n" | "\r\n" | "\r" | "\f")
    val whitespace: P[Unit] = P(" " | "\t" | newline)
    val comment: P[Unit] = P("--" ~ (!newline ~ AnyChar).rep ~ newline)
    val noTrace: P[Unit] = (comment | whitespace).rep
  }

  val Whitespace: WhitespaceApi.Wrapper = WhitespaceApi.Wrapper {
    import fastparse.all._
    NoTrace(ParsersForNoTrace.noTrace)
  }

  import Whitespace._
  import fastparse.noApi._

  implicit class RichParser[T](parser: fastparse.core.Parser[T, Char, String]) {
    def entireInput: P[T] = parser ~ End
  }

  def keyword(k: String): P[Unit] = P(IgnoreCase(k))

  val digit: P[Unit] = P(CharIn('0' to '9'))
  val character: P[Unit] = P(CharIn('a' to 'z', 'A' to 'Z'))
  val identifier: P[Unit] = P(character ~~ P(character | digit | "_").repX)
  val escapedIdentifier: P[String] = identifier.! | P("`" ~ CharsWhile(_ != '`').! ~ "`")
  val label: P[String] = P(":" ~ (identifier.! | escapedIdentifier))
}
