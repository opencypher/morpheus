/**
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
package org.opencypher.parser

import cats.data.NonEmptyList
import CypherExpression._

object CypherAst {

  case class Cypher(statement: Statement) extends CypherTree

  sealed trait Query extends Statement

  sealed trait RegularQuery extends Query

  case class SingleQuery(
    clauses: NonEmptyList[Clause]
  ) extends RegularQuery

  case class Union(all: Boolean, left: RegularQuery, right: SingleQuery) extends RegularQuery

  sealed trait ProcedureInvocation {
    def procedureName: ProcedureName
  }

  case class StandaloneCall(procedureInvocation: ProcedureInvocation, yieldItems: List[YieldItem]) extends Query

  case class SetClause(setItems: NonEmptyList[SetItem]) extends UpdatingClause

  case class RelationshipDetail(
    maybeVariable: Option[Variable],
    relationshipTypes: List[RelTypeName],
    maybeRangeLiteral: Option[RangeLiteral],
    maybeProperties: Option[Properties]
  ) extends CypherTree

  case class Return(distinct: Boolean, returnBody: ReturnBody) extends Clause

  case class CaseAlternatives(whenExpr: Expression, thenExpr: Expression) extends CypherTree

  sealed trait RelationshipPattern extends CypherTree {

    def relationshipDetail: RelationshipDetail

  }

  case class LeftToRight(relationshipDetail: RelationshipDetail) extends RelationshipPattern

  case class RightToLeft(relationshipDetail: RelationshipDetail) extends RelationshipPattern

  case class Undirected(relationshipDetail: RelationshipDetail) extends RelationshipPattern

  case class With(distinct: Boolean, returnBody: ReturnBody, maybeWhere: Option[Where]) extends Clause

  case class PatternElement(nodePattern: NodePattern, patternElementChain: List[PatternElementChain]) extends CypherTree

  case class Pattern(patternParts: NonEmptyList[PatternPart]) extends CypherTree

  case class PatternElementChain(relationshipPattern: RelationshipPattern, nodePattern: NodePattern) extends CypherTree

  case class SortItem(expression: Expression, maybeSortOrder: Option[SortOrder]) extends CypherTree

  sealed trait SortOrder

  case object Ascending extends SortOrder

  case object Descending extends SortOrder

  case class Limit(expression: Expression) extends CypherTree

  case class ReturnItems(star: Boolean, returnItems: List[ReturnItem]) extends CypherTree

  case class Remove(removeItems: NonEmptyList[RemoveItem]) extends UpdatingClause

  case class FunctionName(namespace: List[String], name: String) extends CypherTree {
    override def toString: String = s"FunctionName(${if (namespace.isEmpty) name else s"${namespace.mkString(".")}.$name"})"
  }

  sealed trait UpdatingStartClause extends Clause

  case class YieldItem(maybeProcedureResultField: Option[ProcedureResultField], variable: Variable) extends CypherTree

  sealed trait SetItem extends CypherTree

  case class SetProperty(expression: Property, value: Expression) extends SetItem

  case class SetVariable(variable: Variable, value: Expression) extends SetItem

  case class SetAdditionalItem(variable: Variable, value: Expression) extends SetItem

  case class SetLabels(variable: Variable, nodeLabels: NonEmptyList[String]) extends SetItem

  case class ProcedureName(namespace: List[String], name: String) extends CypherTree {
    override def toString: String = s"ProcedureName(${if (namespace.isEmpty) name else s"${namespace.mkString(".")}.$name"})"
  }

  case class ImplicitProcedureInvocation(procedureName: ProcedureName) extends ProcedureInvocation

  case class Where(expression: Expression) extends CypherTree

  case class Delete(detach: Boolean, expressions: NonEmptyList[Expression]) extends UpdatingClause

  case class RangeLiteral(maybeFrom: Option[IntegerLiteral], maybeTo: Option[IntegerLiteral]) extends CypherTree

  case class PatternPart(maybeVariable: Option[Variable], element: PatternElement) extends CypherTree

  sealed trait Statement extends CypherTree

  sealed trait Clause extends CypherTree

  sealed trait ReadingClause extends Clause

  case class ProcedureResultField(name: String) extends CypherTree

  case class RelTypeName(relTypeName: String) extends CypherTree

  case class InQueryCall(
    explicitProcedureInvocation: ExplicitProcedureInvocation,
    yieldItems: List[YieldItem]
  ) extends ReadingClause

  case class ReturnBody(
    returnItems: ReturnItems,
    maybeOrderBy: Option[OrderBy],
    maybeSkip: Option[Skip],
    maybeLimit: Option[Limit]
  ) extends CypherTree

  case class OrderBy(sortItems: NonEmptyList[SortItem]) extends CypherTree

  case class Unwind(expression: Expression, variable: Variable) extends ReadingClause

  case class IdInColl(variable: Variable, expression: Expression) extends CypherTree

  case class Match(optional: Boolean, pattern: Pattern, maybeWhere: Option[Where]) extends ReadingClause

  case class ExplicitProcedureInvocation(
    procedureName: ProcedureName,
    expressions: List[Expression]
  ) extends ProcedureInvocation

  case class Create(pattern: Pattern) extends UpdatingStartClause with UpdatingClause

  sealed trait UpdatingClause extends Clause

  case class Skip(expression: Expression) extends CypherTree

  case class Merge(
    patternPart: PatternPart,
    mergeActions: List[MergeAction]
  ) extends UpdatingStartClause with UpdatingClause

  sealed trait MergeAction extends CypherTree

  case class OnMerge(set: SetClause) extends MergeAction

  case class OnCreate(set: SetClause) extends MergeAction

}