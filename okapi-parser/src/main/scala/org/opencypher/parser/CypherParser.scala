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

import java.lang.Character.UnicodeBlock

import cats.data.NonEmptyList
import fastparse.Parsed.{Failure, Success}
import org.opencypher.okapi.api.exception.CypherException
import org.opencypher.okapi.api.exception.CypherException.ErrorPhase.CompileTime
import org.opencypher.okapi.api.exception.CypherException.ErrorType.SyntaxError
import org.opencypher.okapi.api.exception.CypherException._
import org.opencypher.okapi.api.value.CypherValue.CypherValue
import fastparse._
import org.opencypher.parser.CypherAst._
import org.opencypher.parser.CypherExpression._

import scala.annotation.tailrec

case class ParsingException(override val detail: ErrorDetails)
  extends CypherException(SyntaxError, CompileTime, detail)

object Whitespace {
  def newline[_: P]: P[Unit] = P("\n" | "\r\n" | "\r" | "\f")
  def invisible[_: P]: P[Unit] = P(" " | "\t" | newline)
  def comment[_: P]: P[Unit] = P("--" ~ (!newline ~ AnyChar).rep ~ newline)

  implicit val cypherWhitespace: P[_] => P[Unit] = { implicit ctx: ParsingRun[_] => (comment | invisible).repX(0) }
}

object CypherParser {
  import Whitespace.cypherWhitespace

  def parseCypher9(query: String, parameters: Map[String, CypherValue] = Map.empty): CypherTree = {
    val parseResult = parse(query, cypherQueryEntireInput(_), verboseFailures = true)
    val ast = parseResult match {
      case Success(v, _) =>
        println(query)
        v
      case Failure(expected, index, extra) =>
        println(query)
        val i = extra.input
        val before = index - math.max(index - 20, 0)
        val after = math.min(index + 20, i.length) - index
        println(extra.input.slice(index - before, index + after).replace('\n', ' '))
        println("~" * before + "^" + "~" * after)

        val maybeNextCharacter = if (index < i.length) Some(i(index)) else None
        maybeNextCharacter match {
          case Some(c) if UnicodeBlock.of(c) != UnicodeBlock.BASIC_LATIN =>
            throw ParsingException(InvalidUnicodeCharacter(s"'$c' is not a valid unicode character"))
          case _ =>
        }

        // TODO: Parse without end instead and check end index?
        val parsedQueryBeforeFailure: Option[CypherTree] = {
          parse(i, cypherQuery(_), verboseFailures = true) match {
            case Success(v, _) => Some(v)
            case _ => None
          }
        }

        parsedQueryBeforeFailure match {
          case Some(s: StandaloneCall) =>
            throw ParsingException(InvalidArgumentPassingMode(
              s"\n`${s.procedureInvocation.procedureName}` needs explicit arguments, unless it is used in a standalone call."))
          case _ =>
        }

        val lastSuccessfulParse: Option[CypherTree] = {
          @tailrec def lastChild(ast: CypherTree): CypherTree = {
            if (ast.children.length == 0) ast else lastChild(ast.children.last)
          }

          parsedQueryBeforeFailure.map(lastChild)
        }

        lastSuccessfulParse match {
          case Some(_: NumberLiteral) if maybeNextCharacter.isDefined =>
            throw ParsingException(InvalidNumberLiteral(
              s"\n'${maybeNextCharacter.get}' is not a valid next character for a number literal."))
          case _ =>
        }

        val traced = extra.trace()
        traced.stack.last match {
          case (parser, _) if parser == "relationshipDetail" && maybeNextCharacter.isDefined =>
            throw ParsingException(InvalidRelationshipPattern(
              s"'${maybeNextCharacter.get}' is not a valid part of a relationship pattern"))
          case other =>
            println(s"Last stack frame: $other")
        }

        println(s"Expected=$expected")
        println(s"Message=${traced.msg}")
        println(s"Aggregate message=${traced.aggregateMsg}")
        println(s"Stack=${traced.stack}")
        throw new Exception(expected)
    }
    ast.show()
    ast
  }

  implicit class ListParserOps[E](val p: P[Seq[E]]) extends AnyVal {
    def toList: P[List[E]] = p.map(l => l.toList)
    def toNonEmptyList: P[NonEmptyList[E]] = p.map(l => NonEmptyList.fromListUnsafe(l.toList))
  }

  implicit class OptionalParserOps[E](val p: P[Option[E]]) extends AnyVal {
    def toBoolean: P[Boolean] = p.map(_.isDefined)
  }

  def K(s: String)(implicit ctx: P[Any]): P[Unit] = IgnoreCase(s) ~~ &(CharIn(" \t\n\r\f") | End)

  def cypherQueryEntireInput[_: P]: P[Query] = P(Start ~ cypherQuery ~ ";".? ~ End)

  def cypherQuery[_: P]: P[Query] = P(regularQuery | standaloneCall)

  def regularQuery[_: P]: P[RegularQuery] = P(
    singleQuery ~ union.rep
  ).map { case (lhs, unions) =>
    if (unions.isEmpty) lhs
    else unions.foldLeft(lhs: RegularQuery) { case (currentQuery, nextUnion) => nextUnion(currentQuery) }
  }

  def union[_: P]: P[RegularQuery => Union] = P(
    K("UNION") ~ K("ALL").!.?.toBoolean ~ singleQuery
  ).map { case (all, rhs) => lhs => Union(all, lhs, rhs) }

  def singleQuery[_: P]: P[SingleQuery] = P(clause.rep(1).toNonEmptyList).map(SingleQuery)

  def clause[_: P]: P[Clause] = P(
    merge
      | delete
      | set
      | create
      | remove
      | withClause
      | matchClause
      | unwind
      | inQueryCall
      | returnClause
  )

  def withClause[_: P]: P[With] = P(K("WITH") ~/ K("DISTINCT").!.?.toBoolean ~/ returnBody ~ where.?).map(With.tupled)

  def matchClause[_: P]: P[Match] = P(K("OPTIONAL").!.?.toBoolean ~ K("MATCH") ~/ pattern ~ where.?).map(Match.tupled)

  def unwind[_: P]: P[Unwind] = P(K("UNWIND") ~/ expression ~ K("AS") ~/ variable).map(Unwind.tupled)

  def merge[_: P]: P[Merge] = P(K("MERGE") ~/ patternPart ~ mergeAction.rep.toList).map(Merge.tupled)

  def mergeAction[_: P]: P[MergeAction] = P(onMatchMergeAction | onCreateMergeAction)

  // TODO: Optimize
  def onMatchMergeAction[_: P]: P[OnMerge] = P(K("ON") ~ K("MATCH") ~/ set).map(OnMerge)

  // TODO: Optimize
  def onCreateMergeAction[_: P]: P[OnCreate] = P(K("ON") ~ K("CREATE") ~/ set).map(OnCreate)

  def create[_: P]: P[Create] = P(IgnoreCase("CREATE") ~/ pattern).map(Create)

  def set[_: P]: P[SetClause] = P(K("SET") ~/ setItem.rep(1).toNonEmptyList).map(SetClause)

  def setItem[_: P]: P[SetItem] = P(
    setProperty
      | setVariable
      | addToVariable
      | setLabels
  )

  def setProperty[_: P]: P[SetProperty] = P(propertyExpression ~ "=" ~ expression).map(SetProperty.tupled)

  def setVariable[_: P]: P[SetVariable] = P(variable ~ "=" ~ expression).map(SetVariable.tupled)

  def addToVariable[_: P]: P[SetAdditionalItem] = P(variable ~ "+=" ~ expression).map(SetAdditionalItem.tupled)

  def setLabels[_: P]: P[SetLabels] = P(variable ~ nodeLabel.rep(1).toNonEmptyList).map(SetLabels.tupled)

  def delete[_: P]: P[Delete] = P(
    K("DETACH").!.?.toBoolean ~ K("DELETE") ~/ expression.rep(1, ",").toNonEmptyList
  ).map(Delete.tupled)

  def remove[_: P]: P[Remove] = P(K("REMOVE") ~/ removeItem.rep(1, ",").toNonEmptyList).map(Remove)

  def removeItem[_: P]: P[RemoveItem] = P(removeNodeVariable | removeProperty)

  def removeNodeVariable[_: P]: P[RemoveNodeVariable] = P(
    variable ~ nodeLabel.rep(1).toNonEmptyList
  ).map(RemoveNodeVariable.tupled)

  def removeProperty[_: P]: P[Property] = P(propertyExpression)

  def inQueryCall[_: P]: P[InQueryCall] = P(K("CALL") ~ explicitProcedureInvocation ~ yieldItems).map(InQueryCall.tupled)

  def standaloneCall[_: P]: P[StandaloneCall] = P(
    K("CALL") ~ procedureInvocation ~ yieldItems
  ).map(StandaloneCall.tupled)

  def procedureInvocation[_: P]: P[ProcedureInvocation] = P(explicitProcedureInvocation | implicitProcedureInvocation)

  def yieldItems[_: P]: P[List[YieldItem]] = P(
    (K("YIELD") ~ (explicitYieldItems | noYieldItems)).?.map(_.getOrElse(Nil))
  )

  def explicitYieldItems[_: P]: P[List[YieldItem]] = P(yieldItem.rep(1, ",").toList)

  def noYieldItems[_: P]: P[List[YieldItem]] = P("-").map(_ => Nil)

  def yieldItem[_: P]: P[YieldItem] = P((procedureResultField ~ K("AS")).? ~ variable).map(YieldItem.tupled)

  def returnClause[_: P]: P[Return] = P(K("RETURN") ~/ K("DISTINCT").!.?.toBoolean ~/ returnBody).map(Return.tupled)

  def returnBody[_: P]: P[ReturnBody] = P(returnItems ~ orderBy.? ~ skip.? ~ limit.?).map(ReturnBody.tupled)

  def returnItems[_: P]: P[ReturnItems] = P(
    ("*" ~/ ("," ~ returnItem.rep(1, ",").toList).?
      .map(_.getOrElse(Nil))).map(ReturnItems(true, _))
      | returnItem.rep(1, ",").toList.map(ReturnItems(false, _))
  )

  def returnItem[_: P]: P[ReturnItem] = P(
    expression ~ alias.?
  ).map {
    case (e, None) => e
    case (e, Some(a)) => a(e)
  }

  def alias[_: P]: P[Expression => Alias] = P(K("AS") ~ variable).map(v => e => Alias(e, v))

  def orderBy[_: P]: P[OrderBy] = P(K("ORDER BY") ~/ sortItem.rep(1, ",").toNonEmptyList).map(OrderBy)

  def skip[_: P]: P[Skip] = P(K("SKIP") ~ expression).map(Skip)

  def limit[_: P]: P[Limit] = P(K("LIMIT") ~ expression).map(Limit)

  def sortItem[_: P]: P[SortItem] = P(
    expression ~ (
                 IgnoreCase("ASCENDING").map(_ => Ascending)
                   | IgnoreCase("ASC").map(_ => Ascending)
                   | IgnoreCase("DESCENDING").map(_ => Descending)
                   | IgnoreCase("DESC").map(_ => Descending)
                 ).?
  ).map(SortItem.tupled)

  def where[_: P]: P[Where] = P(K("WHERE") ~ expression).map(Where)

  def pattern[_: P]: P[Pattern] = P(patternPart.rep(1, ",").toNonEmptyList).map(Pattern)

  def patternPart[_: P]: P[PatternPart] = P((variable ~ "=").? ~ patternElement).map(PatternPart.tupled)

  def patternElement[_: P]: P[PatternElement] = P(
    (nodePattern ~ patternElementChain.rep.toList).map(PatternElement.tupled)
      | "(" ~ patternElement ~ ")"
  )

  def nodePattern[_: P]: P[NodePattern] = P(
    "(" ~ variable.? ~ nodeLabel.rep.toList ~ properties.? ~ ")"
  ).map(NodePattern.tupled)

  def patternElementChain[_: P]: P[PatternElementChain] = P(relationshipPattern ~ nodePattern).map(PatternElementChain.tupled)

  def relationshipPattern[_: P]: P[RelationshipPattern] = P(
    hasLeftArrow ~/ relationshipDetail ~ hasRightArrow
  ).map {
    case (false, detail, true) => LeftToRight(detail)
    case (true, detail, false) => RightToLeft(detail)
    case (_, detail, _) => Undirected(detail)
  }

  def hasLeftArrow[_: P]: P[Boolean] = P(leftArrowHead.!.?.map(_.isDefined) ~ dash)

  def hasRightArrow[_: P]: P[Boolean] = P(dash ~ rightArrowHead.!.?.map(_.isDefined))

  def relationshipDetail[_: P]: P[RelationshipDetail] = P(
    "[" ~/ variable.? ~/ relationshipTypes ~/ rangeLiteral.? ~/ properties.? ~/ "]"
  ).?.map(_.map(RelationshipDetail.tupled).getOrElse(RelationshipDetail(None, List.empty, None, None)))

  def properties[_: P]: P[Properties] = P(mapLiteral | parameter)

  def relationshipTypes[_: P]: P[List[RelTypeName]] = P(
    (":" ~ relTypeName.rep(1, "|" ~ ":".?)).toList.?.map(_.getOrElse(Nil))
  )

  def nodeLabel[_: P]: P[String] = P(":" ~ labelName.!)

  def rangeLiteral[_: P]: P[RangeLiteral] = P(
    "*" ~/ integerLiteral.? ~ (".." ~ integerLiteral.?).?.map(_.flatten)
  ).map(RangeLiteral.tupled)

  def labelName[_: P]: P[Unit] = P(schemaName)

  def relTypeName[_: P]: P[RelTypeName] = P(schemaName).!.map(RelTypeName)

  def expression[_: P]: P[Expression] = P(orExpression)

  def orExpression[_: P]: P[Expression] = P(
    xorExpression ~ (K("OR") ~ xorExpression).rep
  ).map { case (lhs, rhs) =>
    if (rhs.isEmpty) lhs
    else Or(NonEmptyList(lhs, rhs.toList))
  }

  def xorExpression[_: P]: P[Expression] = P(
    andExpression ~ (K("XOR") ~ andExpression).rep
  ).map { case (lhs, rhs) =>
    if (rhs.isEmpty) lhs
    else Xor(NonEmptyList(lhs, rhs.toList))
  }

  def andExpression[_: P]: P[Expression] = P(
    notExpression ~ (K("AND") ~ notExpression).rep
  ).map { case (lhs, rhs) =>
    if (rhs.isEmpty) lhs
    else And(NonEmptyList(lhs, rhs.toList))
  }

  def notExpression[_: P]: P[Expression] = P(
    K("NOT").!.rep.map(_.length) ~ comparisonExpression
  ).map { case (notCount, expr) =>
    notCount % 2 match {
      case 0 => expr
      case 1 => Not(expr)
    }
  }

  def comparisonExpression[_: P]: P[Expression] = P(
    addOrSubtractExpression ~ partialComparisonExpression.rep
  ).map { case (lhs, ops) =>
    if (ops.isEmpty) lhs
    else ops.foldLeft(lhs) { case (currentLhs, nextOp) => nextOp(currentLhs) }
  }

  def partialComparisonExpression[_: P]: P[Expression => Expression] = P(
    partialEqualComparison.map(rhs => (lhs: Expression) => Equal(lhs, rhs))
      | partialNotEqualExpression.map(rhs => (lhs: Expression) => Not(Equal(lhs, rhs)))
      | partialLessThanExpression.map(rhs => (lhs: Expression) => LessThan(lhs, rhs))
      | partialGreaterThanExpression.map(rhs => (lhs: Expression) => Not(LessThanOrEqual(lhs, rhs)))
      | partialLessThanOrEqualExpression.map(rhs => (lhs: Expression) => LessThanOrEqual(lhs, rhs))
      | partialGreaterThanOrEqualExpression.map(rhs => (lhs: Expression) => Not(LessThan(lhs, rhs)))
  )

  def partialEqualComparison[_: P]: P[Expression] = P("=" ~ addOrSubtractExpression)

  def partialNotEqualExpression[_: P]: P[Expression] = P("<>" ~ addOrSubtractExpression)

  def partialLessThanExpression[_: P]: P[Expression] = P("<" ~ addOrSubtractExpression)

  def partialGreaterThanExpression[_: P]: P[Expression] = P(">" ~ addOrSubtractExpression)

  def partialLessThanOrEqualExpression[_: P]: P[Expression] = P("<=" ~ addOrSubtractExpression)

  def partialGreaterThanOrEqualExpression[_: P]: P[Expression] = P(">=" ~ addOrSubtractExpression)

  def addOrSubtractExpression[_: P]: P[Expression] = P(
    multiplyDivideModuloExpression ~ (partialAddExpression | partialSubtractExpression).rep
  ).map { case (lhs, ops) =>
    if (ops.isEmpty) lhs
    else ops.foldLeft(lhs) { case (currentLhs, partialExpression) => partialExpression(currentLhs) }
  }

  def partialAddExpression[_: P]: P[Expression => Expression] = P(
    "+" ~ multiplyDivideModuloExpression
  ).map(rhs => (lhs: Expression) => Add(lhs, rhs))

  def partialSubtractExpression[_: P]: P[Expression => Expression] = P(
    "-" ~ multiplyDivideModuloExpression
  ).map(rhs => (lhs: Expression) => Subtract(lhs, rhs))

  def multiplyDivideModuloExpression[_: P]: P[Expression] = P(
    powerOfExpression ~
      (partialMultiplyExpression | partialDivideExpression | partialModuloExpression).rep
  ).map { case (lhs, ops) =>
    if (ops.isEmpty) lhs
    else ops.foldLeft(lhs) { case (currentLhs, nextOp) => nextOp(currentLhs) }
  }

  def partialMultiplyExpression[_: P]: P[Expression => Expression] = P(
    "*" ~ powerOfExpression
  ).map(rhs => lhs => Multiply(lhs, rhs))

  def partialDivideExpression[_: P]: P[Expression => Expression] = P(
    "/" ~ powerOfExpression
  ).map(rhs => lhs => Divide(lhs, rhs))

  def partialModuloExpression[_: P]: P[Expression => Expression] = P(
    "%" ~ powerOfExpression
  ).map(rhs => lhs => Modulo(lhs, rhs))

  def powerOfExpression[_: P]: P[Expression] = P(
    unaryAddOrSubtractExpression ~ ("^" ~ unaryAddOrSubtractExpression).rep
  ).map { case (lhs, ops) =>
    if (ops.isEmpty) lhs
    else { // "power of" is right associative => reverse the order of the "power of" expressions before fold left
      val head :: tail = (lhs :: ops.toList).reverse
      tail.foldLeft(head) { case (currentExponent, nextBase) => PowerOf(nextBase, currentExponent) }
    }
  }

  def unaryAddOrSubtractExpression[_: P]: P[Expression] = P(
    (P("+").map(_ => 0) | P("-").map(_ => 1)).rep.map(unarySubtractions => unarySubtractions.sum % 2 match {
      case 0 => false
      case 1 => true
    }) ~ stringListNullOperatorExpression
  ).map { case (unarySubtract, expr) =>
    if (unarySubtract) UnarySubtract(expr)
    else expr
  }

  def stringListNullOperatorExpression[_: P]: P[Expression] = P(
    propertyOrLabelsExpression
      ~ (stringOperatorExpression | listOperatorExpression | nullOperatorExpression).rep
  ).map {
    case (expr, ops) =>
      if (ops.isEmpty) expr
      else StringListNullOperator(expr, NonEmptyList.fromListUnsafe(ops.toList))
  }

  def stringOperatorExpression[_: P]: P[StringOperator] = P(
    startsWith
      | endsWith
      | contains
  )

  def in[_: P]: P[In] = P(K("IN") ~ propertyOrLabelsExpression).map(In)

  def startsWith[_: P]: P[StartsWith] = P(K("STARTS WITH") ~ propertyOrLabelsExpression).map(StartsWith)

  def endsWith[_: P]: P[EndsWith] = P(K("ENDS WITH") ~ propertyOrLabelsExpression).map(EndsWith)

  def contains[_: P]: P[Contains] = P(K("CONTAINS") ~ propertyOrLabelsExpression).map(Contains)

  def listOperatorExpression[_: P]: P[ListOperator] = P(
    in
      | singleElementListOperatorExpression
      | rangeListOperatorExpression
  )

  def singleElementListOperatorExpression[_: P]: P[SingleElementListOperator] = P(
    "[" ~ expression ~ "]"
  ).map(SingleElementListOperator)

  def rangeListOperatorExpression[_: P]: P[RangeListOperator] = P(
    "[" ~ expression.? ~ ".." ~ expression.? ~ "]"
  ).map(RangeListOperator.tupled)

  def nullOperatorExpression[_: P]: P[NullOperator] = P(isNull | isNotNull)

  def isNull[_: P]: P[IsNull.type] = P(K("IS NULL")).map(_ => IsNull)

  // TODO: Simplify
  def isNotNull[_: P]: P[IsNotNull.type] = P(K("IS NOT NULL")).map(_ => IsNotNull)

  def propertyOrLabelsExpression[_: P]: P[Expression] = P(
    atom ~ propertyLookup.rep.toList ~ nodeLabel.rep.toList
  ).map { case (a, pls, nls) =>
    if (pls.isEmpty && nls.isEmpty) a
    else PropertyOrLabels(a, pls, nls)
  }

  def atom[_: P]: P[Atom] = P(
    literal
      | parameter
      | caseExpression
      | patternComprehension
      | listComprehension
      | (IgnoreCase("COUNT") ~ "(" ~ "*" ~ ")").map(_ => CountStar)
      | (IgnoreCase("ALL") ~ "(" ~ filterExpression ~ ")").map(FilterAll)
      | (IgnoreCase("ANY") ~ "(" ~ filterExpression ~ ")").map(FilterAny)
      | (IgnoreCase("NONE") ~ "(" ~ filterExpression ~ ")").map(FilterNone)
      | (IgnoreCase("SINGLE") ~ "(" ~ filterExpression ~ ")").map(FilterSingle)
      | relationshipsPattern
      | parenthesizedExpression
      | functionInvocation
      | variable
  )

  def literal[_: P]: P[Literal] = P(
    numberLiteral
      | stringLiteral
      | booleanLiteral
      | IgnoreCase("NULL").map(_ => NullLiteral)
      | mapLiteral
      | listLiteral
  )

  def stringLiteral[_: P]: P[StringLiteral] = P(stringLiteral1 | stringLiteral2)

  def newline[_: P]: P[Unit] = P("\n" | "\r\n" | "\r" | "\f")

  //TODO: Add unicode characters
  def escapedChar[_: P]: P[String] = P("\\" ~~ !(newline | hexDigit) ~~ AnyChar.!)

  def stringLiteralChar(delimiter: String)(implicit ctx: P[Any]): P[String] = P(escapedChar | (!delimiter ~~ AnyChar.!))

  // TODO: Make more efficient
  def stringLiteralWithDelimiter(delimiter: String)(implicit ctx: P[Any]): P[StringLiteral] = P {
    P(delimiter ~~ stringLiteralChar(delimiter).repX.map(_.mkString) ~~ delimiter).map(StringLiteral)
  }

  def stringLiteral1[_: P]: P[StringLiteral] = stringLiteralWithDelimiter("\"")

  def stringLiteral2[_: P]: P[StringLiteral] = stringLiteralWithDelimiter("\'")

  def booleanLiteral[_: P]: P[BooleanLiteral] = P(
    IgnoreCase("TRUE").map(_ => true)
      | IgnoreCase("FALSE").map(_ => false)
  ).map(BooleanLiteral)

  def listLiteral[_: P]: P[ListLiteral] = P("[" ~ NoCut(expression).rep(0, ",").toList ~ "]").map(ListLiteral)

  def parenthesizedExpression[_: P]: P[ParenthesizedExpression] = P("(" ~ expression ~ ")").map(ParenthesizedExpression)

  def relationshipsPattern[_: P]: P[RelationshipsPattern] = P(
    nodePattern ~ patternElementChain.rep(1).toNonEmptyList
  ).map(RelationshipsPattern.tupled)

  def filterExpression[_: P]: P[Filter] = P(idInColl ~ where.?).map(Filter.tupled)

  def idInColl[_: P]: P[IdInColl] = P(variable ~ K("IN") ~ expression).map(IdInColl.tupled)

  def functionInvocation[_: P]: P[FunctionInvocation] = P(
    functionName ~ "(" ~ K("DISTINCT").!.?.toBoolean ~ expression.rep(0, ",").toList ~ ")"
  ).map(FunctionInvocation.tupled)

  def functionName[_: P]: P[FunctionName] = P(
    (namespace ~~ symbolicName.!)
      | K("EXISTS").map(_ => Nil -> "EXISTS")
  ).map(FunctionName.tupled)

  def explicitProcedureInvocation[_: P]: P[ExplicitProcedureInvocation] = P(
    procedureName ~ "(" ~ expression.rep(0, ",").toList ~ ")"
  ).map(ExplicitProcedureInvocation.tupled)

  def implicitProcedureInvocation[_: P]: P[ImplicitProcedureInvocation] = P(procedureName).map(ImplicitProcedureInvocation)

  def procedureResultField[_: P]: P[ProcedureResultField] = P(symbolicName.!).map(ProcedureResultField)

  def procedureName[_: P]: P[ProcedureName] = P(namespace ~ symbolicName.!).map(ProcedureName.tupled)

  def namespace[_: P]: P[List[String]] = P((symbolicName.! ~ ".").rep.toList)

  def listComprehension[_: P]: P[ListComprehension] = P(
    "[" ~ filterExpression ~ ("|" ~ expression).? ~ "]"
  ).map(ListComprehension.tupled)

  def patternComprehension[_: P]: P[PatternComprehension] = P(
    "[" ~ (variable ~ "=").? ~ NoCut(relationshipsPattern) ~ (K("WHERE") ~ expression).? ~ "|" ~ expression ~ "]"
  ).map {
    // Switch parameter order. Required to support automated child inference in okapi trees.
    case (first, second, third, fourth) => PatternComprehension(first, second, fourth, third)
  }

  def propertyLookup[_: P]: P[String] = P("." ~ propertyKeyName)

  def caseExpression[_: P]: P[CaseExpression] = P(
    K("CASE") ~ expression.? ~ caseAlternatives.rep(1).toNonEmptyList ~ (K("ELSE") ~ expression).? ~ K("END")
  ).map(CaseExpression.tupled)

  def caseAlternatives[_: P]: P[CaseAlternatives] = P(
    K("WHEN") ~ expression ~ K("THEN") ~ expression
  ).map(CaseAlternatives.tupled)

  def variable[_: P]: P[Variable] = P(symbolicName).!.map(Variable)

  def numberLiteral[_: P]: P[NumberLiteral] = P(
    doubleLiteral
      | integerLiteral
  )

  def propertyLiteral[_: P]: P[PropertyLiteral] = P(propertyKeyName ~ ":" ~ expression).map(PropertyLiteral.tupled)

  def mapLiteral[_: P]: P[MapLiteral] = P(
    "{" ~ propertyLiteral.rep(0, ",") ~ "}"
  ).map(_.toList).map(MapLiteral)

  def parameter[_: P]: P[Parameter] = P("$" ~ (symbolicName.!.map(ParameterName) | indexParameter))

  def indexParameter[_: P]: P[IndexParameter] = P(decimalInteger).!.map(_.toLong).map(IndexParameter)

  def propertyExpression[_: P]: P[Property] = P(
    atom ~ propertyLookup.rep(1).toNonEmptyList
  ).map(Property.tupled)

  def propertyKeyName[_: P]: P[String] = P(schemaName.!)

  def integerLiteral[_: P]: P[IntegerLiteral] = P(
    hexInteger.!.map(h => java.lang.Long.parseLong(h.drop(2), 16))
      | octalInteger.!.map(java.lang.Long.parseLong(_, 8))
      | decimalInteger.!.map(java.lang.Long.parseLong)
  ).map(IntegerLiteral)

  def hexInteger[_: P]: P[Unit] = P("0x" ~ hexDigit.rep(1))

  def decimalInteger[_: P]: P[Unit] = P(zeroDigit | nonZeroDigit ~ digit.rep)

  def octalInteger[_: P]: P[Unit] = P(zeroDigit ~ octDigit.rep(1))

  def hexLetter[_: P]: P[Unit] = P(CharIn("a-fA-F"))

  def hexDigit[_: P]: P[Unit] = P(digit | hexLetter)

  def digit[_: P]: P[Unit] = P(zeroDigit | nonZeroDigit)

  def nonZeroDigit[_: P]: P[Unit] = P(CharIn("1-9"))

  def octDigit[_: P]: P[Unit] = P(CharIn("0-7"))

  def zeroDigit[_: P]: P[Unit] = P("0")

  // TODO: Simplify
  def doubleLiteral[_: P]: P[DoubleLiteral] = P(
    exponentDecimalReal.!
      | regularDecimalReal.!
  ).map { d =>
    val parsed = d.toDouble
    if (parsed == Double.PositiveInfinity || parsed == Double.NegativeInfinity) {
      throw ParsingException(FloatingPointOverflow(s"'$d' is too large to represent as a Java Double"))
    } else {
      DoubleLiteral(parsed)
    }
  }

  def exponentDecimalReal[_: P]: P[Unit] = P(
    ((digit.rep(1) ~ "." ~ digit.rep(1))
      | ("." ~ digit.rep(1))
      | digit.rep(1)
    ) ~ CharIn("eE") ~ "-".? ~ digit.rep(1)
  )

  def regularDecimalReal[_: P]: P[Unit] = P(digit.rep ~ "." ~ digit.rep(1))

  def schemaName[_: P]: P[Unit] = P(symbolicName | reservedWord)

  def reservedWord[_: P]: P[Unit] = P(
    K("ALL")
      | K("ASC")
      | K("ASCENDING")
      | K("BY")
      | K("CREATE")
      | K("DELETE")
      | K("DESC")
      | K("DESCENDING")
      | K("DETACH")
      | K("EXISTS")
      | K("LIMIT")
      | K("MATCH")
      | K("MERGE")
      | K("ON")
      | K("OPTIONAL")
      | K("ORDER")
      | K("REMOVE")
      | K("RETURN")
      | K("SET")
      | K("SKIP")
      | K("WHERE")
      | K("WITH")
      | K("UNION")
      | K("UNWIND")
      | K("AND")
      | K("AS")
      | K("CONTAINS")
      | K("DISTINCT")
      | K("ENDS")
      | K("IN")
      | K("IS")
      | K("NOT")
      | K("OR")
      | K("STARTS")
      | K("XOR")
      | K("FALSE")
      | K("TRUE")
      | K("NULL")
      | K("CONSTRAINT")
      | K("DO")
      | K("FOR")
      | K("REQUIRE")
      | K("UNIQUE")
      | K("CASE")
      | K("WHEN")
      | K("THEN")
      | K("ELSE")
      | K("END")
      | K("MANDATORY")
      | K("SCALAR")
      | K("OF")
      | K("ADD")
      | K("DROP")
  )

  def symbolicName[_: P]: P[Unit] = P(unescapedSymbolicName | escapedSymbolicName | hexLetter)

  def unescapedSymbolicName[_: P]: P[Unit] = P(identifierPart.repX(1))

  // TODO: Constrain
  def identifierStart[_: P]: P[Unit] = P(CharIn("a-zA-Z"))

  // TODO: Constrain
  def identifierPart[_: P]: P[Unit] = P(CharIn("a-zA-Z0-9_"))

  //     * Any character except "`", enclosed within `backticks`. Backticks are escaped with double backticks. */
  def escapedSymbolicName[_: P]: P[Unit] = P(("`" ~ escapedSymbolicName0.rep ~ "`").repX(1))

  // TODO: Constrain
  def escapedSymbolicName0[_: P]: P[Unit] = P(CharIn("a-zA-Z0-9_"))

  def leftArrowHead[_: P]: P[Unit] = P(
    "<"
      | "⟨"
      | "〈"
      | "﹤"
      | "＜"
  )

  def rightArrowHead[_: P]: P[Unit] = P(
    ">"
      | "⟩"
      | "〉"
      | "﹥"
      | "＞"
  )

  def dash[_: P]: P[Unit] = P(
    "-"
      | "­"
      | "‐"
      | "‑"
      | "‒"
      | "–"
      | "—"
      | "―"
      | "−"
      | "﹘"
      | "﹣"
      | "－")

}
