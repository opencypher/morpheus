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
package org.opencypher.caps.impl.parse.rewriter

import org.neo4j.cypher.internal.frontend.v3_3.{CypherException, InputPosition, Rewriter, bottomUp}
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.helpers.FreshIdNameGenerator

case class normalizeReturnClauses(mkException: (String, InputPosition) => CypherException) extends Rewriter {

  def apply(that: AnyRef): AnyRef = instance.apply(that)

  private val clauseRewriter: (Clause => Seq[Clause]) = {
    case clause @ Return(distinct, ri @ ReturnItems(_, items), gri, None, skip, limit, _) =>
      val (aliasProjection, finalProjection) = items.map {
        // avoid aliasing of primitive expressions (i.e. variables and properties)
        case item @ AliasedReturnItem(Variable(_), Variable(_)) =>
          val returnItem = UnaliasedReturnItem(item.variable, item.variable.name)(item.position)
          (returnItem, returnItem)

        case item @ AliasedReturnItem(Property(_, _), _) =>
          (item, AliasedReturnItem(item.variable, item.variable)(item.position))

        case item @ UnaliasedReturnItem(Variable(_), _) =>
          (item, item)

        case item @ UnaliasedReturnItem(Property(_, _), _) =>
          (item, item)

        // alias remaining return items
        case item =>
          val returnColumn = item.alias match {
            case Some(alias) => alias
            case None        => Variable(item.name)(item.expression.position.bumped())
          }

          val newVariable = Variable(FreshIdNameGenerator.name(item.expression.position))(item.expression.position)

          (
            AliasedReturnItem(item.expression, newVariable)(item.position),
            AliasedReturnItem(newVariable.copyId, returnColumn)(item.position))
      }.unzip

      val introducedVariables = if (ri.includeExisting) aliasProjection.collect {
        case AliasedReturnItem(_, variable) => variable.name
      }.toSet
      else Set.empty[String]

      if (aliasProjection.forall {
            case _: UnaliasedReturnItem => true
            case _                      => false
          }) {
        Seq(clause)
      } else {
        Seq(
          With(
            distinct = distinct,
            returnItems = ri.copy(items = aliasProjection)(ri.position),
            gri.getOrElse(PassAllGraphReturnItems(clause.position)),
            orderBy = None,
            skip = skip,
            limit = limit,
            where = None
          )(clause.position),
          Return(
            distinct = distinct,
            returnItems = ri.copy(items = finalProjection)(ri.position),
            gri,
            orderBy = None,
            skip = None,
            limit = None,
            excludedNames = introducedVariables)(clause.position)
        )
      }

    case clause =>
      Seq(clause)
  }

  private val instance: Rewriter = bottomUp(Rewriter.lift {
    case query @ SingleQuery(clauses) =>
      query.copy(clauses = clauses.flatMap(clauseRewriter))(query.position)
  })
}
