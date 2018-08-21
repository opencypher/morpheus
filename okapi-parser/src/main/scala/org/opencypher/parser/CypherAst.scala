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

import cats.data.NonEmptyList
import org.opencypher.okapi.trees.AbstractTreeNode

abstract class CypherAst extends AbstractTreeNode[CypherAst]

case class Cypher(statement: Statement) extends CypherAst

trait Query extends CypherAst with Statement

case class Union(all: Boolean, left: Query, right: Query) extends Query

case class ReadOnlyEnd(readPart: ReadPart, returnClause: Return) extends Query

case class ReadUpdateEnd(
  readingClauses: NonEmptyList[ReadingClause],
  updatingClauses: NonEmptyList[UpdatingClause],
  maybeReturn: Option[Return]
) extends Query

case class UpdatingEnd(
  updatingStartClause: UpdatingStartClause,
  updatingClauses: List[UpdatingClause],
  maybeReturn: Option[Return]
) extends Query

case class MultiPartQuery(
  readPartOrUpdatingPart: ReadPartOrUpdatingClause,
  withClause: With,
  readUpdateWithParts: List[ReadUpdateWithPart],
  singlePartQuery: Query
) extends Query

trait ReadPartOrUpdatingClause

case class UpdatingStart(
  updatingStartClause: UpdatingStartClause,
  updatingPart: UpdatingPart
) extends ReadPartOrUpdatingClause

case class ReadUpdateWithPart(readPart: ReadPart, updatingPart: UpdatingPart, withPart: With)

trait ProcedureInvocation

case class StandaloneCall(procedureInvocation: ProcedureInvocation, maybeYieldItems: Option[YieldItems]) extends Query

trait Expression extends CypherAst with ReturnItem

case class OrExpression(expressions: NonEmptyList[Expression]) extends Expression

case class XorExpression(expressions: NonEmptyList[Expression]) extends Expression

case class AndExpression(expressions: NonEmptyList[Expression]) extends Expression

case class NotExpression(expression: Expression) extends Expression

case class EqualExpression(left: Expression, right: Expression) extends Expression

case class LessThanExpression(left: Expression, right: Expression) extends Expression

case class LessThanOrEqualExpression(left: Expression, right: Expression) extends Expression

case class AddExpression(left: Expression, right: Expression) extends Expression

case class SubtractExpression(left: Expression, right: Expression) extends Expression

case class MultiplyExpression(left: Expression, right: Expression) extends Expression

case class DivideExpression(left: Expression, right: Expression) extends Expression

case class ModuloExpression(left: Expression, right: Expression) extends Expression

case class PowerOfExpression(expressions: NonEmptyList[Expression]) extends Expression

case class UnarySubtractExpression(expression: Expression) extends Expression

case class StringListNullOperatorExpression(
  propertyOrLabelsExpression: Expression,
  operatorExpressions: NonEmptyList[OperatorExpression]
) extends Expression

trait OperatorExpression extends Expression

trait StringOperatorExpression extends OperatorExpression {
  def propertyOrLabelsExpression: PropertyOrLabelsExpression
}

case class In(propertyOrLabelsExpression: PropertyOrLabelsExpression) extends StringOperatorExpression
case class StartsWith(propertyOrLabelsExpression: PropertyOrLabelsExpression) extends StringOperatorExpression
case class EndsWith(propertyOrLabelsExpression: PropertyOrLabelsExpression) extends StringOperatorExpression
case class Contains(propertyOrLabelsExpression: PropertyOrLabelsExpression) extends StringOperatorExpression

trait ListOperatorExpression extends OperatorExpression

case class SingleElementListOperatorExpression(expression: Expression) extends ListOperatorExpression

case object EmptyRangeListOperatorExpression extends ListOperatorExpression

case class FromRangeListOperatorExpression(from: Expression) extends ListOperatorExpression

case class ToRangeListOperatorExpression(to: Expression) extends ListOperatorExpression

case class FromToRangeListOperatorExpression(from: Expression, to: Expression) extends ListOperatorExpression

trait NullOperatorExpression extends OperatorExpression

case object IsNull extends NullOperatorExpression

case object IsNotNull extends NullOperatorExpression

trait Properties extends CypherAst

case class Alias(expr: Expression, as: Variable) extends ReturnItem

case class SetClause(setItems: NonEmptyList[SetItem]) extends CypherAst with MergeAction with UpdatingClause

case class RelationshipDetail(
  maybeVariable: Option[Variable],
  maybeRelationshipTypes: Option[RelationshipTypes],
  maybeRangeLiteral: Option[RangeLiteral],
  maybeProperties: Option[Properties]
) extends CypherAst

trait Atom extends Expression

case class Return(distinct: Boolean, returnBody: ReturnBody) extends CypherAst

case class CaseAlternatives(whenExpr: Expression, thenExpr: Expression) extends CypherAst

sealed trait RelationshipPattern extends CypherAst {

  def relationshipDetail: Option[RelationshipDetail]

}

case class LeftToRight(relationshipDetail: Option[RelationshipDetail]) extends RelationshipPattern

case class RightToLeft(relationshipDetail: Option[RelationshipDetail]) extends RelationshipPattern

case class Undirected(relationshipDetail: Option[RelationshipDetail]) extends RelationshipPattern

case class With(distinct: Boolean, returnBody: ReturnBody, maybeWhere: Option[Where]) extends CypherAst

case class PatternElement(nodePattern: NodePattern, patternElementChain: List[PatternElementChain]) extends CypherAst

case class PropertyExpression(
  atom: Atom,
  propertyLookups: NonEmptyList[PropertyLookup]
) extends CypherAst with RemoveItem

case class RelationshipTypes(relTypeNames: NonEmptyList[RelTypeName]) extends CypherAst

trait Parameter extends CypherAst with Properties with Atom

case class Pattern(patternParts: NonEmptyList[PatternPart]) extends CypherAst

case class PropertyOrLabelsExpression(
  atom: Atom,
  propertyLookups: List[PropertyLookup],
  maybeNodeLabels: Option[NodeLabels]
) extends CypherAst with Expression

case class PatternElementChain(relationshipPattern: RelationshipPattern, nodePattern: NodePattern) extends CypherAst

case class NodePattern(
  maybeVariable: Option[Variable],
  maybeNodeLabels: Option[NodeLabels],
  maybeProperties: Option[Properties]
) extends CypherAst

case class Variable(name: String) extends CypherAst with Atom

case class NodeLabel(nodeLabel: String) extends CypherAst

case class SymbolicName(value: String) extends CypherAst with Parameter with FunctionName

case class SortItem(expression: Expression, maybeSortOrder: Option[SortOrder]) extends CypherAst

trait SortOrder

case object Ascending extends SortOrder

case object Descending extends SortOrder

case class Limit(expression: Expression) extends CypherAst

case class ParenthesizedExpression(expression: Expression) extends CypherAst with Atom

case class ReturnItems(star: Boolean, returnItems: List[ReturnItem]) extends CypherAst

case class Remove(removeItems: NonEmptyList[RemoveItem]) extends CypherAst with UpdatingClause

trait Literal extends CypherAst with Atom

trait FunctionName extends CypherAst

trait PropertyLookup extends CypherAst {
  def propertyKeyName: String
}

case class NodeLabels(nodeLabels: NonEmptyList[NodeLabel]) extends CypherAst

trait NumberLiteral extends CypherAst with Literal

case class FilterExpression(idInColl: IdInColl, maybeWhere: Option[Where]) extends CypherAst with Atom

/** Order switched between `expression` and `maybeWhereExpression`
  * to avoid children allocation problems between them.
  */
case class PatternComprehension(
  maybeVariable: Option[Variable],
  relationshipsPattern: RelationshipsPattern,
  expression: Expression,
  maybeWhereExpression: Option[Expression]
)

trait UpdatingStartClause extends CypherAst

case class YieldItem(maybeProcedureResultField: Option[ProcedureResultField], variable: Variable) extends CypherAst

trait SetItem extends CypherAst

case class SetProperty(expression: PropertyExpression, value: Expression) extends SetItem

case class SetVariable(variable: Variable, value: Expression) extends SetItem

case class SetAdditionalItem(variable: Variable, value: Expression) extends SetItem

case class SetLabels(variable: Variable, nodeLabels: NodeLabels) extends SetItem

case class ProcedureName(namespace: Namespace, symbolicName: SymbolicName) extends CypherAst

trait RemoveItem extends CypherAst

trait MergeAction extends CypherAst

case class ImplicitProcedureInvocation(procedureName: ProcedureName) extends CypherAst with ProcedureInvocation

case class Where(expression: Expression) extends CypherAst

trait ReturnItem extends CypherAst

case class Delete(expressions: NonEmptyList[Expression]) extends CypherAst with UpdatingClause

trait RangeLiteral extends CypherAst {

  def maybeFrom: Option[IntegerLiteral] = None

  def maybeTo: Option[IntegerLiteral] = None

}

case class FromToRange(from: IntegerLiteral, to: IntegerLiteral) extends RangeLiteral {

  override def maybeFrom: Option[IntegerLiteral] = Some(from)

  override def maybeTo: Option[IntegerLiteral] = Some(to)

}

case class FromRange(from: IntegerLiteral) extends RangeLiteral {

  override def maybeFrom: Option[IntegerLiteral] = Some(from)

}

case class ToRange(to: IntegerLiteral) extends RangeLiteral {

  override def maybeTo: Option[IntegerLiteral] = Some(to)

}

case class RelationshipsPattern(
  nodePattern: NodePattern,
  patternElementChains: NonEmptyList[PatternElementChain]
) extends CypherAst with Atom

case class PatternPart(element: PatternElement, maybeVariable: Option[Variable]) extends CypherAst

case class IntegerLiteral(value: Long) extends CypherAst with NumberLiteral

trait Statement extends CypherAst

case class ReadPart(readingClauses: List[ReadingClause]) extends CypherAst with ReadPartOrUpdatingClause

trait ReadingClause extends CypherAst

case class ProcedureResultField(symbolicName: SymbolicName) extends CypherAst

trait DoubleLiteral extends CypherAst with NumberLiteral

case class RelTypeName(relTypeName: String) extends CypherAst

case class Namespace(symbolicNames: List[SymbolicName]) extends CypherAst

case class ListLiteral(listLiterals: List[Expression]) extends CypherAst with Literal

case class MapLiteral(properties: List[(PropertyKeyName, Expression)]) extends CypherAst with Properties with Literal

case class InQueryCall(
  explicitProcedureInvocation: ExplicitProcedureInvocation,
  maybeYieldItems: Option[YieldItems]
) extends CypherAst with ReadingClause

trait YieldItems extends CypherAst

case class FunctionInvocation(
  functionName: FunctionName,
  distinct: Boolean,
  expressions: NonEmptyList[Expression]
) extends CypherAst with Atom

case class ReturnBody(
  returnItems: ReturnItems,
  maybeOrder: Option[Order],
  maybeSkip: Option[Skip],
  maybeLimit: Option[Limit]
) extends CypherAst

case class Order(sortItems: NonEmptyList[SortItem]) extends CypherAst

case class Unwind(expression: Expression, variable: Variable) extends CypherAst with ReadingClause

case class IdInColl(variable: Variable, expression: Expression) extends CypherAst

case class CaseExpression(
  maybeCaseExpression: Option[Expression],
  caseAlternatives: NonEmptyList[CaseAlternatives],
  maybeElseExpression: Option[Expression]
) extends CypherAst with Atom

case class Match(optional: Boolean, pattern: Pattern, maybeWhere: Option[Where]) extends CypherAst with ReadingClause

case class ExplicitProcedureInvocation(
  procedureName: ProcedureName,
  expressions: List[Expression]
) extends CypherAst with ProcedureInvocation

case class Create(pattern: Pattern) extends CypherAst with UpdatingStartClause with UpdatingClause

trait UpdatingClause extends CypherAst

case class UpdatingPart(updatingClauses: List[UpdatingClause]) extends CypherAst

case class Skip(expression: Expression) extends CypherAst

case class ListComprehension(
  filterExpression: FilterExpression,
  maybeExpression: Option[Expression]
) extends CypherAst with Atom

case class Merge(
  patternPart: PatternPart,
  mergeActions: List[MergeAction]
) extends CypherAst with UpdatingStartClause with UpdatingClause

case class PropertyKeyName(propertyKeyName: String) extends CypherAst with PropertyLookup
