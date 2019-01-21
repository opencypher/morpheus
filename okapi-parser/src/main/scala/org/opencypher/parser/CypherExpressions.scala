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
import org.opencypher.okapi.trees.AbstractTreeNode
import org.opencypher.parser.CypherAst._

object CypherExpression {

  // Basic node types

  abstract class CypherTree extends AbstractTreeNode[CypherTree]

  sealed trait ReturnItem extends CypherTree

  sealed trait Expression extends ReturnItem

  sealed trait Atom extends Expression

  sealed trait OperatorExpression extends Expression

  sealed trait ListOperator extends OperatorExpression

  sealed trait StringOperator extends OperatorExpression {
    def expression: Expression
  }

  sealed trait Properties extends CypherTree

  // Variable and alias

  case class Variable(name: String) extends Atom

  case class Alias(expr: Expression, as: Variable) extends ReturnItem

  // Literals

  sealed trait Literal extends Atom

  case class StringLiteral(value: String) extends Literal

  case object NullLiteral extends Literal

  case class BooleanLiteral(value: Boolean) extends Literal

  sealed trait NumberLiteral extends Literal

  case class IntegerLiteral(value: Long) extends NumberLiteral

  case class DoubleLiteral(value: Double) extends NumberLiteral

  case class ListLiteral(listLiterals: List[Expression]) extends Literal

  case class PropertyLiteral(key: String, value: Expression) extends Literal

  case class MapLiteral(properties: List[PropertyLiteral]) extends Properties with Literal

  // Parameters

  sealed trait Parameter extends Properties with Atom

  case class ParameterName(name: String) extends Parameter

  case class IndexParameter(index: Long) extends Parameter

  // Operators

  case class Or(expressions: NonEmptyList[Expression]) extends Expression

  case class Xor(expressions: NonEmptyList[Expression]) extends Expression

  case class And(expressions: NonEmptyList[Expression]) extends Expression

  case class Not(expression: Expression) extends Expression

  case class Equal(left: Expression, right: Expression) extends Expression

  case class LessThan(left: Expression, right: Expression) extends Expression

  case class LessThanOrEqual(left: Expression, right: Expression) extends Expression

  case class Add(left: Expression, right: Expression) extends Expression

  case class Subtract(left: Expression, right: Expression) extends Expression

  case class Multiply(left: Expression, right: Expression) extends Expression

  case class Divide(left: Expression, right: Expression) extends Expression

  case class Modulo(left: Expression, right: Expression) extends Expression

  case class PowerOf(base: Expression, exponent: Expression) extends Expression

  case class UnarySubtract(expression: Expression) extends Expression

  case class StringListNullOperator(
    propertyOrLabelsExpression: Expression,
    operatorExpressions: NonEmptyList[OperatorExpression]
  ) extends Expression

  case class StartsWith(expression: Expression) extends StringOperator
  case class EndsWith(expression: Expression) extends StringOperator
  case class Contains(expression: Expression) extends StringOperator

  case class In(expression: Expression) extends ListOperator
  case class SingleElementListOperator(expression: Expression) extends ListOperator

  case class RangeListOperator(maybeFrom: Option[Expression], maybeTo: Option[Expression]) extends ListOperator

  sealed trait NullOperator extends OperatorExpression

  case object IsNull extends NullOperator

  case object IsNotNull extends NullOperator

  case object CountStar extends Atom

  sealed trait RemoveItem extends CypherTree

  case class Property(
    atom: Atom,
    propertyLookups: NonEmptyList[String]
  ) extends RemoveItem

  case class PropertyOrLabels(
    atom: Atom,
    propertyLookups: List[String],
    nodeLabels: List[String]
  ) extends Expression

  case class RemoveNodeVariable(variable: Variable, nodeLabels: NonEmptyList[String]) extends RemoveItem

  /** Order switched between `expression` and `maybeWhereExpression`
    * to avoid children allocation problems between them.
    */
  case class PatternComprehension(
    maybeVariable: Option[Variable],
    relationshipsPattern: RelationshipsPattern,
    expression: Expression,
    maybeWhereExpression: Option[Expression]
  ) extends Atom

  case class RelationshipsPattern(
    nodePattern: NodePattern,
    patternElementChains: NonEmptyList[PatternElementChain]
  ) extends Atom

  case class NodePattern(
    maybeVariable: Option[Variable],
    nodeLabels: List[String],
    maybeProperties: Option[Properties]
  ) extends CypherTree

  case class FunctionInvocation(
    functionName: FunctionName,
    distinct: Boolean,
    expressions: List[Expression]
  ) extends Atom

  case class CaseExpression(
    maybeCaseExpression: Option[Expression],
    caseAlternatives: NonEmptyList[CaseAlternatives],
    maybeDefault: Option[Expression]
  ) extends Atom

  case class Filter(idInColl: IdInColl, maybeWhere: Option[Where]) extends Atom

  case class FilterAll(filterExpression: Filter) extends Atom

  case class FilterAny(filterExpression: Filter) extends Atom

  case class FilterNone(filterExpression: Filter) extends Atom

  case class FilterSingle(filterExpression: Filter) extends Atom

  case class ListComprehension(
    filterExpression: Filter,
    maybeExpression: Option[Expression]
  ) extends Atom

  case class ParenthesizedExpression(expression: Expression) extends Atom

}
