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
package org.opencypher.okapi.ir.impl.parse.rewriter.legacy

import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.expressions.Expression
import org.neo4j.cypher.internal.v4_0.util.{Rewriter, bottomUp}

case object projectFreshSortExpressions extends Rewriter {
  override def apply(that: AnyRef): AnyRef = instance(that)
  private val clauseRewriter: Clause => Seq[Clause] = {
    case clause@With(_, _, None, _, _, None) =>
      Seq(clause)
    case clause@With(_, ri: ReturnItems, orderBy, skip, limit, where) =>
      val allAliases = ri.aliases
      val passedThroughAliases = ri.passedThrough
      val evaluatedAliases = allAliases -- passedThroughAliases
      if (evaluatedAliases.isEmpty) {
        Seq(clause)
      } else {
        val nonItemDependencies = orderBy.map(_.dependencies).getOrElse(Set.empty) ++
          skip.map(_.dependencies).getOrElse(Set.empty) ++
          limit.map(_.dependencies).getOrElse(Set.empty) ++
          where.map(_.dependencies).getOrElse(Set.empty)
        val dependenciesFromPreviousScope = nonItemDependencies -- allAliases
        val passedItems = dependenciesFromPreviousScope.map(AliasedReturnItem(_))
        val outputItems = allAliases.toIndexedSeq.map(AliasedReturnItem(_))
        val result = Seq(
          clause.copy(returnItems = ri.mapItems(originalItems => originalItems ++ passedItems), orderBy = None, skip = None, limit = None, where = None)(clause.position),
          clause.copy(distinct = false, returnItems = ri.mapItems(_ => outputItems))(clause.position)
        )
        result
      }
    case clause =>
      Seq(clause)
  }
  private val rewriter = Rewriter.lift {
    case query@SingleQuery(clauses) =>
      query.copy(clauses = clauses.flatMap(clauseRewriter))(query.position)
  }
  private val instance: Rewriter = bottomUp(rewriter, _.isInstanceOf[Expression])
}
