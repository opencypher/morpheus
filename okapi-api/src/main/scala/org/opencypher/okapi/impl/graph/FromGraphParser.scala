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
package org.opencypher.okapi.impl.graph

import org.opencypher.okapi.api.value.CypherValue.CypherString
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.v9_0.ast
import org.opencypher.v9_0.ast.{CatalogName, FromGraph, GraphLookup, ViewInvocation}
import org.opencypher.v9_0.parser.Query
import org.parboiled.scala.parserunners.ReportingParseRunner

object FromGraphParser {

  private val queryParser = new Query {}

  def parse(name: String, value: String): ast.FromGraph = {
    ReportingParseRunner(queryParser.GraphOrView).run(value).result match {
      case Some(fromGraph) => fromGraph
      case None => throw IllegalArgumentException("A valid FROM GRAPH expression", s"Parameter $name with value $value")
    }
  }

  implicit class RichCatalogName(val c: CatalogName) extends AnyVal {

    def toCypherString: CypherString = {
      c.parts.mkString(".")
    }

  }

  implicit class RichFromGraph(val f: FromGraph) extends AnyVal {

    def toCypherString: CypherString = {
      f match {
        case GraphLookup(cn) => cn.toCypherString
        case ViewInvocation(cn, params) =>
          s"${cn.toCypherString}(${params.map(_.toCypherString).mkString(", ")})"
      }
    }
  }

}
