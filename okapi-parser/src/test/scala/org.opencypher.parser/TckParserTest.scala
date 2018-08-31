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
package org.opencypher.parser

import org.opencypher.okapi.api.exception.CypherException
import org.opencypher.okapi.tck.test.ScenariosFor
import org.opencypher.okapi.tck.test.Tags.WhiteList
import org.opencypher.tools.tck.api._
import org.opencypher.tools.tck.values.{CypherValue => TckValue}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.{FunSpec, Matchers, Tag}

import scala.util.{Failure, Success, Try}

class TckParserTest extends FunSpec with Matchers with MockitoSugar {

  object TckParserTag extends Tag("TckParser")

  val scenarios: ScenariosFor = ScenariosFor(blacklist = Set.empty[String])

  forAll(scenarios.whiteList) { scenario =>
    it(s"[${WhiteList.name}] $scenario", WhiteList, TckParserTag) {
      import scenario.ScenarioFailedException
      var isParserError = false
      Try {
        scenario(new Graph {
          override def cypher(query: String, params: Map[String, TckValue], meta: QueryType): Result = {
            meta match {
              case InitQuery | SideEffectQuery => CypherValueRecords(List.empty, List.empty)
              case ExecQuery =>
                Try(Cypher10Parser.parse(query)) match {
                  case Success(_) =>
                    CypherValueRecords(List.empty, List.empty)
                  case Failure(e) =>
                    e match {
                      case c: CypherException =>
                        isParserError = true
                        // Special case for errors that are hard to generate with ANTLR
//                        val detail = if (c.detail.getClass.getSimpleName == "ParsingError") {
//                          "InvalidUnicodeLiteral"
//                        } else {
//                          c.detail.getClass.getSimpleName
//                        }
                        println(c.detail)
                        resultFromError(
                          ExecutionFailed(c.errorType.toString, c.phase.toString, c.detail.getClass.getSimpleName)
                        )
                      case other => throw other
                    }
                }
            }
          }
        }).execute()
      } match {
        case Failure(f@ScenarioFailedException(_)) if isParserError => throw f
        case Failure(f@ScenarioFailedException(_)) =>
        case Failure(other) => throw other
        case _ =>
      }
    }
  }
}
