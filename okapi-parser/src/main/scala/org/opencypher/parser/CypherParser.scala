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
import fastparse.WhitespaceApi
import fastparse.core.Frame
import fastparse.core.Parsed.{Failure, Success}
import org.opencypher.okapi.api.exception.CypherException
import org.opencypher.okapi.api.exception.CypherException.ErrorPhase.CompileTime
import org.opencypher.okapi.api.exception.CypherException.ErrorType.SyntaxError
import org.opencypher.okapi.api.exception.CypherException._
import org.opencypher.okapi.api.value.CypherValue.CypherValue

import scala.annotation.tailrec

case class ParsingException(override val detail: ErrorDetails)
  extends CypherException(SyntaxError, CompileTime, detail)

//noinspection ForwardReference
object CypherParser {

  def parse(query: String, parameters: Map[String, CypherValue] = Map.empty): CypherAst = {
    val parseResult = CypherParser.cypher.parse(query)
    val ast = parseResult match {
      case Success(v, _) =>
        println(query)
        v
      case f@Failure(p, index, extra) =>
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

        val lastSuccessfulParse: Option[CypherAst] = {
          @tailrec def lastChild(ast: CypherAst): CypherAst = {
            if (ast.children.length == 0) ast else lastChild(ast.children.last)
          }

          statement.parseInput(i) match {
            case Success(v, _) =>
              val lastSuccess = lastChild(v)
              lastSuccess.show()
              Some(lastSuccess)
            case _ => None
          }
        }

        lastSuccessfulParse match {
          case Some(_: NumberLiteral) if maybeNextCharacter.isDefined =>
            throw ParsingException(InvalidNumberLiteral(
              s"'${maybeNextCharacter.get}' is not a valid next character for a number literal"))
          case _ =>
        }

        extra.traced.stack.last match {
          case Frame(_, parser) if parser.toString == "relationshipDetail" && maybeNextCharacter.isDefined =>
            throw ParsingException(InvalidRelationshipPattern(
              s"'${maybeNextCharacter.get}' is not a valid part of a relationship pattern"))
          case other =>
            println(other)
        }

        println(s"Last named parser on stack=${extra.traced.stack.last}")
        println(s"Failed parser: $p at index $index")
        println(s"Expected=${extra.traced.expected}")
        println(s"Stack=${extra.traced.stack}")
        //println(extra.traced.trace)
        throw new Exception(f.toString)
    }
    ast.show()
    ast
  }

  val White: WhitespaceApi.Wrapper = WhitespaceApi.Wrapper {
    import fastparse.all._

    val newline = P("\n" | "\r\n" | "\r" | "\f")
    val whitespace = P(" " | "\t" | newline)


    //  val WHITESPACE = SPACE
    //   | TAB
    //     | LF
    //     | VT
    //     | FF
    //     | CR
    //     | FS
    //     | GS
    //     | RS
    //     | US
    //     | " "
    //   | "᠎"
    //   | " "
    //   | " "
    //   | " "
    //   | " "
    //   | " "
    //   | " "
    //   | " "
    //   | " "
    //   | " "
    //   | " "
    //   | " "
    //   | " "
    //   | " "
    //   | "　"
    //   | " "
    //   | " "
    //   | " "
    //   | Comment

    //val Comment = P( "/*" ( Comment_1 | ( "*" Comment_2 ) )* "*/" )
    //   | ( "" ( Comment_3 )* CR? ( LF | EOF ) )

    //    val comment = P("--" ~ (!newline ~ AnyChar).rep ~ newline)
    //    NoTrace((comment | whitespace).rep)
    NoTrace(whitespace.rep)
  }

  import White._
  import fastparse.noApi._

  implicit class ListParserOps[E](val p: P[Seq[E]]) extends AnyVal {
    def toList: P[List[E]] = p.map(l => l.toList)
    def toNonEmptyList: P[NonEmptyList[E]] = p.map(l => NonEmptyList.fromListUnsafe(l.toList))
  }

  implicit class OptionalParserOps[E](val p: P[Option[E]]) extends AnyVal {
    def toBoolean: P[Boolean] = p.map(_.isDefined)
  }

  def K(s: String): P[Unit] = P(IgnoreCase(s) ~~ &(CharIn(" \t\n\r\f") | End))

  val cypher: P[Cypher] = P(statement ~ ";".? ~ End).map(Cypher)

  val statement: P[Statement] = P(query)

  val query: P[Query] = P(regularQuery | standaloneCall)

  val regularQuery: P[RegularQuery] = P(
    singleQuery ~ union.rep
  ).map { case (lhs, unions) =>
    if (unions.isEmpty) lhs
    else unions.foldLeft(lhs: RegularQuery) { case (currentQuery, nextUnion) => nextUnion(currentQuery) }
  }

  val union: P[RegularQuery => Union] = P(
    K("UNION") ~ K("ALL").!.?.toBoolean ~ singleQuery
  ).map { case (all, rhs) => lhs => Union(all, lhs, rhs) }

  val singleQuery: P[SingleQuery] = P(singlePartQuery | multiPartQuery)

  val singlePartQuery: P[SinglePartQuery] = P(
    (readingClause.rep.toList ~ returnClause).map(ReadingQuery.tupled)
      | (readingClause.rep.toList ~ updatingClause.rep(min = 1).toNonEmptyList ~ returnClause.?).map(UpdatingQuery.tupled)
  )

  val multiPartQuery: P[MultiPartQuery] = P(readUpdateWith.rep(min = 1).toNonEmptyList ~ singlePartQuery).map(MultiPartQuery.tupled)

  val readUpdateWith: P[ReadUpdateWith] = P(
    readingClause.rep.toList ~ updatingClause.rep.toList ~ withClause
  ).map(ReadUpdateWith.tupled)

  val updatingClause: P[UpdatingClause] = P(
    create
      | merge
      | delete
      | set
      | remove
  )

  val readingClause: P[ReadingClause] = P(
    matchClause
      | unwind
      | inQueryCall
  )

  val withClause: P[With] = P(K("WITH") ~/ K("DISTINCT").!.?.toBoolean ~/ returnBody ~ where.?).map(With.tupled)

  val matchClause: P[Match] = P(K("OPTIONAL").!.?.toBoolean ~ K("MATCH") ~/ pattern ~ where.?).map(Match.tupled)

  val unwind: P[Unwind] = P(K("UNWIND") ~/ expression ~ K("AS") ~/ variable).map(Unwind.tupled)

  val merge: P[Merge] = P(K("MERGE") ~/ patternPart ~ mergeAction.rep.toList).map(Merge.tupled)

  val mergeAction: P[MergeAction] = P(onMatchMergeAction | onCreateMergeAction)

  // TODO: Optimize
  val onMatchMergeAction: P[OnMerge] = P(K("ON") ~ K("MATCH") ~/ set).map(OnMerge)

  // TODO: Optimize
  val onCreateMergeAction: P[OnCreate] = P(K("ON") ~ K("CREATE") ~/ set).map(OnCreate)

  val create: P[Create] = P(IgnoreCase("CREATE") ~/ pattern).map(Create)

  val set: P[SetClause] = P(K("SET") ~/ setItem.rep(min = 1).toNonEmptyList).map(SetClause)

  val setItem: P[SetItem] = P(
    setProperty
      | setVariable
      | addToVariable
      | setLabels
  )

  val setProperty: P[SetProperty] = P(propertyExpression ~ "=" ~ expression).map(SetProperty.tupled)

  val setVariable: P[SetVariable] = P(variable ~ "=" ~ expression).map(SetVariable.tupled)

  val addToVariable: P[SetAdditionalItem] = P(variable ~ "+=" ~ expression).map(SetAdditionalItem.tupled)

  val setLabels: P[SetLabels] = P(variable ~ nodeLabel.rep(min = 1).toNonEmptyList).map(SetLabels.tupled)

  val delete: P[Delete] = P(
    K("DETACH").!.?.toBoolean ~ K("DELETE") ~/ expression.rep(min = 1, sep = ",").toNonEmptyList
  ).map(Delete.tupled)

  val remove: P[Remove] = P(K("REMOVE") ~/ removeItem.rep(min = 1, sep = ",").toNonEmptyList).map(Remove)

  val removeItem: P[RemoveItem] = P(removeNodeVariable | removeProperty)

  val removeNodeVariable: P[RemoveNodeVariable] = P(
    variable ~ nodeLabel.rep(min = 1).toNonEmptyList
  ).map(RemoveNodeVariable.tupled)

  val removeProperty: P[PropertyExpression] = P(propertyExpression)

  val inQueryCall: P[InQueryCall] = P(K("CALL") ~ explicitProcedureInvocation ~ yieldItems).map(InQueryCall.tupled)

  val standaloneCall: P[StandaloneCall] = P(
    K("CALL") ~ procedureInvocation ~ yieldItems
  ).map(StandaloneCall.tupled)

  val procedureInvocation: P[ProcedureInvocation] = P(explicitProcedureInvocation | implicitProcedureInvocation)

  val yieldItems: P[List[YieldItem]] = P(
    (K("YIELD") ~/ (explicitYieldItems | noYieldItems)).?.map(_.getOrElse(Nil))
  )

  val explicitYieldItems: P[List[YieldItem]] = P(yieldItem.rep(min = 1, sep = ",").toList)

  val noYieldItems: P[List[YieldItem]] = P("-").map(_ => Nil)

  val yieldItem: P[YieldItem] = P((procedureResultField ~ K("AS")).? ~ variable).map(YieldItem.tupled)

  val returnClause: P[Return] = P(K("RETURN") ~/ K("DISTINCT").!.?.toBoolean ~/ returnBody).map(Return.tupled)

  val returnBody: P[ReturnBody] = P(returnItems ~ orderBy.? ~ skip.? ~ limit.?).map(ReturnBody.tupled)

  val returnItems: P[ReturnItems] = P(
    ("*" ~/ ("," ~ returnItem.rep(min = 1, sep = ",").toList).?
      .map(_.getOrElse(Nil))).map(ReturnItems(true, _))
      | returnItem.rep(min = 1, sep = ",").toList.map(ReturnItems(false, _))
  )

  val returnItem: P[ReturnItem] = P(
    expression ~ alias.?
  ).map {
    case (e, None) => e
    case (e, Some(a)) => a(e)
  }

  val alias: P[Expression => Alias] = P(K("AS") ~ variable).map(v => e => Alias(e, v))

  val orderBy: P[OrderBy] = P(K("ORDER BY") ~/ sortItem.rep(min = 1, sep = ",").toNonEmptyList).map(OrderBy)

  val skip: P[Skip] = P(K("SKIP") ~ expression).map(Skip)

  val limit: P[Limit] = P(K("LIMIT") ~ expression).map(Limit)

  val sortItem: P[SortItem] = P(
    expression ~ (
                 IgnoreCase("ASCENDING").map(_ => Ascending)
                   | IgnoreCase("ASC").map(_ => Ascending)
                   | IgnoreCase("DESCENDING").map(_ => Descending)
                   | IgnoreCase("DESC").map(_ => Descending)
                 ).?
  ).map(SortItem.tupled)

  val where: P[Where] = P(K("WHERE") ~ expression).map(Where)

  val pattern: P[Pattern] = P(patternPart.rep(min = 1, sep = ",").toNonEmptyList).map(Pattern)

  val patternPart: P[PatternPart] = P((variable ~ "=").? ~ patternElement).map(PatternPart.tupled)

  val patternElement: P[PatternElement] = P(
    (nodePattern ~ patternElementChain.rep.toList).map(PatternElement.tupled)
      | "(" ~ patternElement ~ ")"
  )

  val nodePattern: P[NodePattern] = P(
    "(" ~ variable.? ~ nodeLabel.rep.toList ~ properties.? ~ ")"
  ).map(NodePattern.tupled)

  val patternElementChain: P[PatternElementChain] = P(relationshipPattern ~ nodePattern).map(PatternElementChain.tupled)

  val relationshipPattern: P[RelationshipPattern] = P(
    hasLeftArrow ~ relationshipDetail ~ hasRightArrow
  ).map {
    case (false, detail, true) => LeftToRight(detail)
    case (true, detail, false) => RightToLeft(detail)
    case (_, detail, _) => Undirected(detail)
  }

  val hasLeftArrow: P[Boolean] = P(leftArrowHead.!.?.map(_.isDefined) ~ dash)

  val hasRightArrow: P[Boolean] = P(dash ~ rightArrowHead.!.?.map(_.isDefined))

  val relationshipDetail: P[RelationshipDetail] = P(
    "[" ~ variable.? ~ relationshipTypes ~ rangeLiteral.? ~ properties.? ~ "]"
  ).?.map(_.map(RelationshipDetail.tupled).getOrElse(RelationshipDetail(None, List.empty, None, None)))

  val properties: P[Properties] = P(mapLiteral | parameter)

  val relationshipTypes: P[List[RelTypeName]] = P(
    (":" ~ relTypeName.rep(min = 1, sep = "|" ~ ":".?)).toList.?.map(_.getOrElse(Nil))
  )

  val nodeLabel: P[NodeLabel] = P(":" ~ labelName.!).map(NodeLabel)

  val rangeLiteral: P[RangeLiteral] = P(
    "*" ~/ integerLiteral.? ~ (".." ~ integerLiteral.?).?.map(_.flatten)
  ).map(RangeLiteral.tupled)

  val labelName: P[Unit] = P(schemaName)

  val relTypeName: P[RelTypeName] = P(schemaName).!.map(RelTypeName)

  val expression: P[Expression] = P(orExpression)

  val orExpression: P[Expression] = P(
    xorExpression ~ (K("OR") ~ xorExpression).rep
  ).map { case (lhs, rhs) =>
    if (rhs.isEmpty) lhs
    else OrExpression(NonEmptyList(lhs, rhs.toList))
  }

  val xorExpression: P[Expression] = P(
    andExpression ~ (K("XOR") ~ andExpression).rep
  ).map { case (lhs, rhs) =>
    if (rhs.isEmpty) lhs
    else XorExpression(NonEmptyList(lhs, rhs.toList))
  }

  val andExpression: P[Expression] = P(
    notExpression ~ (K("AND") ~ notExpression).rep
  ).map { case (lhs, rhs) =>
    if (rhs.isEmpty) lhs
    else AndExpression(NonEmptyList(lhs, rhs.toList))
  }

  val notExpression: P[Expression] = P(
    K("NOT").!.rep.map(_.length) ~ comparisonExpression
  ).map { case (notCount, expr) =>
    notCount % 2 match {
      case 0 => expr
      case 1 => NotExpression(expr)
    }
  }

  val comparisonExpression: P[Expression] = P(
    addOrSubtractExpression ~ partialComparisonExpression.rep
  ).map { case (lhs, ops) =>
    if (ops.isEmpty) lhs
    else ops.foldLeft(lhs) { case (currentLhs, nextOp) => nextOp(currentLhs) }
  }

  val partialComparisonExpression: P[Expression => Expression] = P(
    partialEqualComparison.map(rhs => (lhs: Expression) => EqualExpression(lhs, rhs))
      | partialNotEqualExpression.map(rhs => (lhs: Expression) => NotExpression(EqualExpression(lhs, rhs)))
      | partialLessThanExpression.map(rhs => (lhs: Expression) => LessThanExpression(lhs, rhs))
      | partialGreaterThanExpression.map(rhs => (lhs: Expression) => NotExpression(LessThanOrEqualExpression(lhs, rhs)))
      | partialLessThanOrEqualExpression.map(rhs => (lhs: Expression) => LessThanOrEqualExpression(lhs, rhs))
      | partialGreaterThanOrEqualExpression.map(rhs => (lhs: Expression) => NotExpression(LessThanExpression(lhs, rhs)))
  )

  val partialEqualComparison: P[Expression] = P("=" ~ addOrSubtractExpression)

  val partialNotEqualExpression: P[Expression] = P("<>" ~ addOrSubtractExpression)

  val partialLessThanExpression: P[Expression] = P("<" ~ addOrSubtractExpression)

  val partialGreaterThanExpression: P[Expression] = P(">" ~ addOrSubtractExpression)

  val partialLessThanOrEqualExpression: P[Expression] = P("<=" ~ addOrSubtractExpression)

  val partialGreaterThanOrEqualExpression: P[Expression] = P(">=" ~ addOrSubtractExpression)

  val addOrSubtractExpression: P[Expression] = P(
    multiplyDivideModuloExpression ~ (partialAddExpression | partialSubtractExpression).rep
  ).map { case (lhs, ops) =>
    if (ops.isEmpty) lhs
    else ops.foldLeft(lhs) { case (currentLhs, partialExpression) => partialExpression(currentLhs) }
  }

  val partialAddExpression: P[Expression => Expression] = P(
    "+" ~ multiplyDivideModuloExpression
  ).map(rhs => (lhs: Expression) => AddExpression(lhs, rhs))

  val partialSubtractExpression: P[Expression => Expression] = P(
    "-" ~ multiplyDivideModuloExpression
  ).map(rhs => (lhs: Expression) => SubtractExpression(lhs, rhs))

  val multiplyDivideModuloExpression: P[Expression] = P(
    powerOfExpression ~
      (partialMultiplyExpression | partialDivideExpression | partialModuloExpression).rep
  ).map { case (lhs, ops) =>
    if (ops.isEmpty) lhs
    else ops.foldLeft(lhs) { case (currentLhs, nextOp) => nextOp(currentLhs) }
  }

  val partialMultiplyExpression: P[Expression => Expression] = P(
    "*" ~ powerOfExpression
  ).map(rhs => lhs => MultiplyExpression(lhs, rhs))

  val partialDivideExpression: P[Expression => Expression] = P(
    "/" ~ powerOfExpression
  ).map(rhs => lhs => DivideExpression(lhs, rhs))

  val partialModuloExpression: P[Expression => Expression] = P(
    "%" ~ powerOfExpression
  ).map(rhs => lhs => ModuloExpression(lhs, rhs))

  val powerOfExpression: P[Expression] = P(
    unaryAddOrSubtractExpression ~ ("^" ~ unaryAddOrSubtractExpression).rep
  ).map { case (lhs, ops) =>
    if (ops.isEmpty) lhs
    else { // "power of" is right associative => reverse the order of the "power of" expressions before fold left
      val head :: tail = (lhs :: ops.toList).reverse
      tail.foldLeft(head) { case (currentExponent, nextBase) => PowerOfExpression(nextBase, currentExponent) }
    }
  }

  val unaryAddOrSubtractExpression: P[Expression] = P(
    (P("+").map(_ => 0) | P("-").map(_ => 1)).rep.map(unarySubtractions => unarySubtractions.sum % 2 match {
      case 0 => false
      case 1 => true
    }) ~ stringListNullOperatorExpression
  ).map { case (unarySubtract, expr) =>
    if (unarySubtract) UnarySubtractExpression(expr)
    else expr
  }

  val stringListNullOperatorExpression: P[Expression] = P(
    propertyOrLabelsExpression
      ~ (stringOperatorExpression | listOperatorExpression | nullOperatorExpression).rep
  ).map {
    case (expr, ops) =>
      if (ops.isEmpty) expr
      else StringListNullOperatorExpression(expr, NonEmptyList.fromListUnsafe(ops.toList))
  }

  val stringOperatorExpression: P[StringOperatorExpression] = P(
    in
      | startsWith
      | endsWith
      | contains
  )

  val in: P[In] = P(K("IN") ~ propertyOrLabelsExpression).map(In)

  val startsWith: P[StartsWith] = P(K("STARTS WITH") ~ propertyOrLabelsExpression).map(StartsWith)

  val endsWith: P[EndsWith] = P(K("ENDS WITH") ~ propertyOrLabelsExpression).map(EndsWith)

  val contains: P[Contains] = P(K("CONTAINS") ~ propertyOrLabelsExpression).map(Contains)

  val listOperatorExpression: P[ListOperatorExpression] = P(
    singleElementListOperatorExpression
      | rangeListOperatorExpression
  )

  val singleElementListOperatorExpression: P[SingleElementListOperatorExpression] = P(
    "[" ~ expression ~ "]"
  ).map(SingleElementListOperatorExpression)

  val rangeListOperatorExpression: P[RangeListOperatorExpression] = P(
    "[" ~ expression.? ~ ".." ~ expression.? ~ "]"
  ).map(RangeListOperatorExpression.tupled)

  val nullOperatorExpression: P[NullOperatorExpression] = P(isNull | isNotNull)

  val isNull: P[IsNull.type] = P(K("IS NULL")).map(_ => IsNull)

  // TODO: Simplify
  val isNotNull: P[IsNotNull.type] = P(K("IS NOT NULL")).map(_ => IsNotNull)

  val propertyOrLabelsExpression: P[Expression] = P(
    atom ~ propertyLookup.rep.toList ~ nodeLabel.rep.toList
  ).map { case (a, pls, nls) =>
    if (pls.isEmpty && nls.isEmpty) a
    else PropertyOrLabelsExpression(a, pls, nls)
  }

  val atom: P[Atom] = P(
    literal
      | parameter
      | caseExpression
      | patternComprehension
      | listComprehension
      | (IgnoreCase("COUNT") ~ "(" ~ "*" ~ ")").map(_ => CountStar)
      | (IgnoreCase("FILTER") ~ "(" ~ filterExpression ~ ")").map(Filter)
      | (IgnoreCase("EXTRACT") ~ "(" ~ filterExpression ~ ("|" ~ expression).? ~ ")").map(Extract.tupled)
      | (IgnoreCase("ALL") ~ "(" ~ filterExpression ~ ")").map(FilterAll)
      | (IgnoreCase("ANY") ~ "(" ~ filterExpression ~ ")").map(FilterAny)
      | (IgnoreCase("NONE") ~ "(" ~ filterExpression ~ ")").map(FilterNone)
      | (IgnoreCase("SINGLE") ~ "(" ~ filterExpression ~ ")").map(FilterSingle)
      | relationshipsPattern
      | parenthesizedExpression
      | functionInvocation
      | variable
  )

  val literal: P[Literal] = P(
    numberLiteral
      | stringLiteral
      | booleanLiteral
      | IgnoreCase("NULL").map(_ => NullLiteral)
      | mapLiteral
      | listLiteral
  )

  val stringLiteral: P[StringLiteral] = P(stringLiteral1 | stringLiteral2)

  val newline: P[Unit] = P("\n" | "\r\n" | "\r" | "\f")

  //TODO: Add unicode characters
  val escapedChar: P[String] = P(
    "\\" ~~ !(newline | hexDigit) ~~ AnyChar.!
  )

  // TODO: Make more efficient
  def stringLiteralWithDelimiter(delimiter: String): P[StringLiteral] = P {
    val stringLiteralChar: P[String] = P(escapedChar | !delimiter ~~ AnyChar.!)
    P(delimiter ~~ stringLiteralChar.repX.map(_.mkString) ~~ delimiter).map(StringLiteral)
  }

  val stringLiteral1: P[StringLiteral] = stringLiteralWithDelimiter("\"")

  val stringLiteral2: P[StringLiteral] = stringLiteralWithDelimiter("\'")

  val booleanLiteral: P[BooleanLiteral] = P(
    IgnoreCase("TRUE").map(_ => true)
      | IgnoreCase("FALSE").map(_ => false)
  ).map(BooleanLiteral)

  val listLiteral: P[ListLiteral] = P("[" ~ expression.rep(sep = ",").toList ~ "]").map(ListLiteral)

  val parenthesizedExpression: P[ParenthesizedExpression] = P("(" ~ expression ~ ")").map(ParenthesizedExpression)

  val relationshipsPattern: P[RelationshipsPattern] = P(
    nodePattern ~ patternElementChain.rep(min = 1).toNonEmptyList
  ).map(RelationshipsPattern.tupled)

  val filterExpression: P[FilterExpression] = P(idInColl ~ where.?).map(FilterExpression.tupled)

  val idInColl: P[IdInColl] = P(variable ~ K("IN") ~ expression).map(IdInColl.tupled)

  val functionInvocation: P[FunctionInvocation] = P(
    functionName ~ "(" ~ K("DISTINCT").!.?.toBoolean ~ expression.rep(sep = ",").toList ~ ")"
  ).map(FunctionInvocation.tupled)

  val functionName: P[FunctionName] = P(
    symbolicName.!.map(SymbolicName)
      | K("EXISTS").map(_ => Exists)
  )

  val explicitProcedureInvocation: P[ExplicitProcedureInvocation] = P(
    procedureName ~ "(" ~ expression.rep(sep = ",").toList ~ ")"
  ).map(ExplicitProcedureInvocation.tupled)

  val implicitProcedureInvocation: P[ImplicitProcedureInvocation] = P(procedureName).map(ImplicitProcedureInvocation)

  val procedureResultField: P[ProcedureResultField] = P(symbolicName.!.map(SymbolicName)).map(ProcedureResultField)

  val procedureName: P[ProcedureName] = P(namespace ~ symbolicName.!.map(SymbolicName)).map(ProcedureName.tupled)

  val namespace: P[Namespace] = P((symbolicName.!.map(SymbolicName) ~ ".").rep.toList).map(Namespace)

  val listComprehension: P[ListComprehension] = P(
    "[" ~ filterExpression ~ ("|" ~ expression).? ~ "]"
  ).map(ListComprehension.tupled)

  val patternComprehension: P[PatternComprehension] = P(
    "[" ~ (variable ~ "=").? ~ relationshipsPattern ~ (K("WHERE") ~ expression).? ~ "|" ~ expression ~ "]"
  ).map {
    // Switch parameter order. Required to support automated child inference in okapi trees.
    case (first, second, third, fourth) => PatternComprehension(first, second, fourth, third)
  }

  val propertyLookup: P[PropertyLookup] = P("." ~ propertyKeyName)

  val caseExpression: P[CaseExpression] = P(
    K("CASE") ~ expression.? ~ caseAlternatives.rep(min = 1).toNonEmptyList ~ (K("ELSE") ~ expression).? ~ K("END")
  ).map(CaseExpression.tupled)

  val caseAlternatives: P[CaseAlternatives] = P(
    K("WHEN") ~ expression ~ K("THEN") ~ expression
  ).map(CaseAlternatives.tupled)

  val variable: P[Variable] = P(symbolicName).!.map(Variable)

  val numberLiteral: P[NumberLiteral] = P(
    doubleLiteral
      | integerLiteral
  )

  val mapLiteral: P[MapLiteral] = P(
    "{" ~ (propertyKeyName ~ ":" ~ expression).rep(sep = ",") ~ "}"
  ).map(_.toList).map(MapLiteral)

  val parameter: P[Parameter] = P("$" ~ (symbolicName.!.map(SymbolicName) | indexParameter))

  val indexParameter: P[IndexParameter] = P(decimalInteger).!.map(_.toLong).map(IndexParameter)

  val propertyExpression: P[PropertyExpression] = P(
    atom ~ propertyLookup.rep(min = 1).toNonEmptyList
  ).map(PropertyExpression.tupled)

  val propertyKeyName: P[PropertyKeyName] = P(schemaName).!.map(PropertyKeyName)

  val integerLiteral: P[IntegerLiteral] = P(
    hexInteger.!.map(h => java.lang.Long.parseLong(h.drop(2), 16))
      | octalInteger.!.map(java.lang.Long.parseLong(_, 8))
      | decimalInteger.!.map(java.lang.Long.parseLong)
  ).map(IntegerLiteral)

  val hexInteger: P[Unit] = P("0x" ~ hexDigit.rep(min = 1))

  val decimalInteger: P[Unit] = P(zeroDigit | nonZeroDigit ~ digit.rep)

  val octalInteger: P[Unit] = P(zeroDigit ~ octDigit.rep(min = 1))

  val hexLetter: P[Unit] = P(CharIn('a' to 'f', 'A' to 'F'))

  val hexDigit: P[Unit] = P(digit | hexLetter)

  val digit: P[Unit] = P(zeroDigit | nonZeroDigit)

  val nonZeroDigit: P[Unit] = P(CharIn('1' to '9'))

  val octDigit: P[Unit] = P(CharIn('0' to '7'))

  val zeroDigit: P[Unit] = P("0")

  // TODO: Simplify
  val doubleLiteral: P[DoubleLiteral] = P(
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

  val exponentDecimalReal: P[Unit] = P(
    ((digit.rep(min = 1) ~ "." ~ digit.rep(min = 1))
      | ("." ~ digit.rep(min = 1))
      | digit.rep(min = 1)
    ) ~ CharIn("eE") ~ "-".? ~ digit.rep(min = 1)
  )

  val regularDecimalReal: P[Unit] = P(digit.rep ~ "." ~ digit.rep(min = 1))

  val schemaName: P[Unit] = P(symbolicName | reservedWord)

  val reservedWord: P[Unit] = P(
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

  val symbolicName: P[Unit] = P(unescapedSymbolicName | escapedSymbolicName | hexLetter
    | K("COUNT")
    | K("FILTER")
    | K("EXTRACT")
    | K("ANY")
    | K("NONE")
    | K("SINGLE")
  )

  val unescapedSymbolicName: P[Unit] = P(identifierPart.repX(min = 1))

  // TODO: Constrain
  val identifierStart: P[Unit] = P(CharIn('a' to 'z', 'A' to 'Z'))

  // TODO: Constrain
  val identifierPart: P[Unit] = P(CharIn('a' to 'z', 'A' to 'Z', '0' to '9', Seq('_')))

  //     * Any character except "`", enclosed within `backticks`. Backticks are escaped with double backticks. */
  val escapedSymbolicName: P[Unit] = P(("`" ~ escapedSymbolicName0.rep ~ "`").repX(min = 1))

  // TODO: Constrain
  val escapedSymbolicName0: P[Unit] = P(CharIn('a' to 'z', 'A' to 'Z', '0' to '9'))

  val leftArrowHead: P[Unit] = P(
    "<"
      | "⟨"
      | "〈"
      | "﹤"
      | "＜"
  )

  val rightArrowHead: P[Unit] = P(
    ">"
      | "⟩"
      | "〉"
      | "﹥"
      | "＞"
  )

  val dash: P[Unit] = P(
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
