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
package org.opencypher.okapi.impl.types

import fastparse.Parsed.{Failure, Success}
import fastparse._
import org.apache.logging.log4j.scala.Logging
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.util.ParserUtils._

object CypherTypeParser extends Logging {

  def parseCypherType(input: String): Option[CypherType] = {
    parse(input, cypherTypeFromEntireInput(_), verboseFailures = true) match {
      case Success(value, _) => Some(value)
      case Failure(expected, index, extra) =>
        val before = index - math.max(index - 20, 0)
        val after = math.min(index + 20, extra.input.length) - index
        val locationPointer =
          s"""|\t${extra.input.slice(index - before, index + after).replace('\n', ' ')}
              |\t${"~" * before + "^" + "~" * after}
           """.stripMargin
        val msg =
          s"""|Failed at index $index:
              |
              |Expected:\t$expected
              |
              |$locationPointer
              |
              |${extra.trace().msg}""".stripMargin
        logger.debug(msg)
        None
    }
  }

  // Basic types
  def STRING[_: P]: P[CTString.type] = IgnoreCase("STRING").map(_ => CTString)
  def INTEGER[_: P]: P[CTInteger.type] = IgnoreCase("INTEGER").map(_ => CTInteger)
  def FLOAT[_: P]: P[CTFloat.type] = IgnoreCase("FLOAT").map(_ => CTFloat)
  def NUMBER[_: P]: P[CTNumber.type ] = IgnoreCase("NUMBER").map(_ => CTNumber)
  def BOOLEAN[_: P]: P[CTBoolean.type] = IgnoreCase("BOOLEAN").map(_ => CTBoolean)
  def TRUE[_: P]: P[CTTrue.type] = IgnoreCase("TRUE").map(_ => CTTrue)
  def FALSE[_: P]: P[CTFalse.type] = IgnoreCase("FALSE").map(_ => CTFalse)
  def ANY[_: P]: P[CTAny.type ] = IgnoreCase("ANY?").map(_ => CTAny)
  def ANYMATERIAL[_: P]: P[CTAnyMaterial.type] = IgnoreCase("ANY").map(_ => CTAnyMaterial)
  def VOID[_: P]: P[CTVoid.type] = IgnoreCase("VOID").map(_ => CTVoid)
  def NULL[_: P]: P[CTNull.type] = IgnoreCase("NULL").map(_ => CTNull)
  def DATE[_: P]: P[CTDate.type] = IgnoreCase("DATE").map(_ => CTDate)
  def LOCALDATETIME[_: P]: P[CTLocalDateTime.type] = IgnoreCase("LOCALDATETIME").map(_ => CTLocalDateTime)
  def BIGDECIMAL[_: P]: P[CTBigDecimal] =
    (IgnoreCase("BIGDECIMAL") ~/ "(" ~/ integer ~/ "," ~/ integer ~/ ")").map { case (s, p) => CTBigDecimal(s, p) }

//  // element types
//  def NODE[_: P]: P[CTNode] = P(
//    IgnoreCase("NODE") ~ ("(" ~/ label.rep ~ ")") ~ ("@" ~/ (identifier | ".").rep.!).?
//  ).map { case (l, mg) => CTNode(l.toSet, mg.map(QualifiedGraphName(_))) }
//
//  def ANYNODE[_: P]: P[CTNode.type] = P(IgnoreCase("NODE").map(_ => CTNode))
//
//  def RELATIONSHIP[_: P]: P[CTRelationship] = P(
//    IgnoreCase("RELATIONSHIP") ~ ("(" ~/ label.rep(sep = "|") ~/ ")") ~ ("@" ~/ (identifier | ".").rep.!).?
//  ).map { case (l, mg) => CTRelationship(l.toSet, mg.map(QualifiedGraphName(_))) }
//
//  def ANYRELATIONSHIP[_: P]: P[CTRelationship] = P(IgnoreCase("RELATIONSHIP").map(_ => CTRelationship))

//  def ELEMENT[_: P]: P[CTUnion] = P(IgnoreCase("ELEMENT").map(_ => CTElement))

  def PATH[_: P]: P[CTPath.type] = P(IgnoreCase("PATH").map(_ => CTPath))

  // container types
  def ANYLIST[_: P]: P[CTList] = P(IgnoreCase("LIST").map(_ => CTList))
  def LIST[_: P]: P[CTList] = P(IgnoreCase("LIST") ~ "(" ~/ cypherType ~/ ")").map(inner => CTList(inner))

  private def mapKey[_: P]: P[String] = P(identifier.! | escapedIdentifier)
  private def kvPair[_: P]: P[(String, CypherType)] = P(mapKey ~/ ":" ~/ cypherType)
  def ANYMAP[_: P]: P[CTMap] = P(IgnoreCase("MAP").map(_ => CTMap))
  def MAP[_: P]: P[CTMap] = P(IgnoreCase("MAP") ~ "(" ~/ kvPair.rep(sep = ",") ~/ ")").map { inner => CTMap(inner.toMap)
  }

  def materialCypherType[_: P]: P[CypherType] = P(
    STRING |
      INTEGER |
      FLOAT |
      NUMBER |
      TRUE |
      FALSE |
      BOOLEAN |
//      NODE |
//      RELATIONSHIP |
//      ELEMENT |
//      ANYNODE |
//      ANYRELATIONSHIP |
      MAP |
      ANYMAP |
      ANYMATERIAL |
      ANY |
      VOID |
      NULL |
      PATH |
      LIST |
      ANYLIST |
      LOCALDATETIME |
      DATE |
      BIGDECIMAL
  )

  def cypherType[_: P]: P[CypherType] = P((materialCypherType ~ "?".!.?.map(_.isDefined)).map {
    case (ct, isNullable) => if (isNullable) ct.nullable else ct
  })

  def cypherTypeFromEntireInput[_: P]: P[CypherType] = Start ~ cypherType ~ End
}
