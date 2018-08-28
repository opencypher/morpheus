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
import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.tree.{ParseTree, RuleNode, TerminalNode}
import org.opencypher.parser
import org.opencypher.parser.CypherParser._

import scala.collection.JavaConverters._
import scala.language.implicitConversions

case object AntlrAstTransformer extends CypherBaseVisitor[CypherAst] {

  implicit class ToNonEmptyList[T](val l: java.util.List[T]) extends AnyVal {
    def toNonEmpty(implicit ctx: ParserRuleContext): NonEmptyList[T] = {
      val maybeNonEmptyList = NonEmptyList.fromList(l.asScala.toList)
      maybeNonEmptyList match {
        case None => illegalState(ctx, l)
        case Some(ls) => ls
      }
    }
  }

  def illegalState(ctx: ParserRuleContext, illegal: Any): Nothing = {
    ctx.show()
    throw new IllegalStateException(s"Context ${ctx.getText} cannot contain $illegal")
  }

  implicit def parseTreeToBoolean(t: ParseTree): Boolean = {
    t != null
  }

  implicit def terminalListToBoolean(t: java.util.List[TerminalNode]): Boolean = {
    t != null && !t.isEmpty
  }

  implicit class RichJavaList[E](val list: java.util.List[E]) extends AnyVal {

    def map[T](f: E => T): List[T] = {
      Option(list).map(_.asScala.toList.map(f(_))).getOrElse(List.empty[T])
    }

    def fold[T](initial: T)(f: (T, E) => T): T = {
      list.asScala.toList.foldLeft(initial)(f)
    }

    def terminalsLike(s: String): List[String] = {
      list.asScala.toList.collect {
        case t: TerminalNode if s.contains(t.getSymbol.getText) => t.getSymbol.getText
      }
    }

    def containsTerminal(s: String): Boolean = {
      list.asScala.toList.exists {
        case t: TerminalNode if t.getSymbol.getText == s => true
        case _ => false
      }

    }
  }

  implicit class RichParserContext(val ctx: ParserRuleContext) {
    def containsTerminal(s: String): Boolean = {
      ctx.children != null && ctx.children.containsTerminal(s)
    }

    def show(indent: Int = 0, levels: Int = 2): Unit = {
      if (levels > 0) {
        println(s"${"\t" * indent}${CypherParser.ruleNames(ctx.getRuleIndex)}")
        ctx.children.asScala.collect { case c: ParserRuleContext => c }.foreach(
          _.show(indent + 1, levels - 1)
        )
      }
    }
  }

  def visitAlternatives[P](either: ParseTree): P = {
    var result: P = null.asInstanceOf[P]
    val n = either.getChildCount
    var i = 0
    while (result == null && i < n) {
      val child = either.getChild(i)
      result = child.accept(this).asInstanceOf[P]
      i += 1
    }
    if (result == null) {
      println(either.getText)
      assert(result != null)
    }
    result
  }

  override def visitOC_Cypher(ctx: CypherParser.OC_CypherContext): Cypher = {
    Cypher(visitOC_Statement(ctx.oC_Statement))
  }

  override def visitOC_Statement(ctx: CypherParser.OC_StatementContext): Statement = {
    visitOC_Query(ctx.oC_Query)
  }

  override def visitOC_Query(ctx: CypherParser.OC_QueryContext): Query = {
    visitAlternatives[Query](ctx)
  }

  override def visitOC_RegularQuery(ctx: CypherParser.OC_RegularQueryContext): RegularQuery = {
    val single = visitOC_SingleQuery(ctx.oC_SingleQuery)
    if (ctx.oC_Union.isEmpty) {
      single
    } else {
      ctx.oC_Union
        .map[(Boolean, SingleQuery)] { sqc =>
        (sqc.ALL: Boolean) -> visitOC_SingleQuery(sqc.oC_SingleQuery)
      }
        .foldLeft(single: RegularQuery) { case (current: RegularQuery, (all, next)) =>
          Union(all, current, next)
        }
    }
  }

  override def visitOC_SingleQuery(ctx: CypherParser.OC_SingleQueryContext): SingleQuery = {
    // Flatten SingleQuery structure into list of clauses
    def visitOC_Clauses(ctx: ParserRuleContext): List[Clause] = {
      ctx match {
        case c: CypherParser.OC_SinglePartQueryContext =>
          visitChildClauses(c)

        case c: CypherParser.OC_MultiPartQueryContext =>
          visitChildClauses(c)

        case c: CypherParser.OC_ReadOnlyEndContext =>
          visitChildClauses(c)

        case c: CypherParser.OC_UpdatingEndContext =>
          visitChildClauses(c)

        case c: CypherParser.OC_ReadUpdateEndContext =>
          visitChildClauses(c)

        case c: CypherParser.OC_ReadPartContext =>
          c.oC_ReadingClause.map(visitOC_ReadingClause)

        case c: CypherParser.OC_UpdatingPartContext =>
          c.oC_UpdatingClause.map(visitOC_UpdatingClause)

        case c: CypherParser.OC_WithContext =>
          List(visitOC_With(c))

        case c: CypherParser.OC_ReturnContext =>
          List(visitOC_Return(c))

        case c: CypherParser.OC_ReadingClauseContext =>
          List(visitAlternatives[ReadingClause](c))

        case c: CypherParser.OC_UpdatingClauseContext =>
          List(visitAlternatives[UpdatingClause](c))

        case c: CypherParser.OC_UpdatingStartClauseContext =>
          List(visitAlternatives[UpdatingStartClause](c))

        case ni =>
          throw new UnsupportedOperationException(s"${CypherParser.ruleNames(ni.getRuleIndex)}")
      }
    }

    def visitChildClauses(ctx: ParserRuleContext): List[Clause] = {
      ctx.children.asScala.collect {
        case childContext: ParserRuleContext => childContext
      }.toList.flatMap(visitOC_Clauses)
    }

    val clauses = visitChildClauses(ctx)
    SingleQuery(NonEmptyList.fromListUnsafe(clauses))
  }

  override def visitOC_Delete(ctx: CypherParser.OC_DeleteContext): Delete = {
    Delete(ctx.DETACH, ctx.oC_Expression.toNonEmpty(ctx).map(visitOC_Expression))
  }

  //  override def visitOC_SinglePartQuery(ctx: CypherParser.OC_SinglePartQueryContext): SinglePartQuery = {
  //    visitAlternatives[SinglePartQuery](ctx)
  //  }

  //  override def visitOC_MultiPartQuery(ctx: CypherParser.OC_MultiPartQueryContext): MultiPartQuery = {
  //    println(ctx.children.asScala)
  //
  ////    val singlePartQuery = visitOC_SinglePartQuery(ctx.oC_SinglePartQuery)
  ////    ctx.children(0) match {
  ////      case readPart: OC_ReadPartContext =>
  ////        ctx.
  ////        val readingClauses = readPart.oC_ReadingClause.map(visitOC_ReadingClause)
  ////
  ////      case updating: OC_UpdatingStartClauseContext =>
  ////
  ////    }
  //    //MultiPartQuery
  //    ???
  //  }

  //  override def visitOC_ReadOnlyEnd(ctx: CypherParser.OC_ReadOnlyEndContext): ReadOnlyEnd = {
  //    val readingClauses = ctx.oC_ReadPart.oC_ReadingClause.map(visitOC_ReadingClause)
  //    ReadOnlyEnd(readingClauses, visitOC_Return(ctx.oC_Return))
  //  }

  override def visitOC_ReadingClause(ctx: CypherParser.OC_ReadingClauseContext): ReadingClause = {
    visitAlternatives(ctx).asInstanceOf[ReadingClause]
  }

  override def visitOC_UpdatingClause(ctx: CypherParser.OC_UpdatingClauseContext): UpdatingClause = {
    visitAlternatives(ctx).asInstanceOf[UpdatingClause]
  }

  override def visitOC_Return(ctx: CypherParser.OC_ReturnContext): Return = {
    Return(ctx.DISTINCT, visitOC_ReturnBody(ctx.oC_ReturnBody))
  }

  override def visitOC_ReturnBody(ctx: CypherParser.OC_ReturnBodyContext): ReturnBody = {
    val returnItems = Option(ctx.oC_ReturnItems)
      .map(visitOC_ReturnItems)
      .getOrElse(ReturnItems.empty)
    ReturnBody(
      returnItems,
      Option(ctx.oC_Order).map(visitOC_Order),
      Option(ctx.oC_Skip).map(visitOC_Skip),
      Option(ctx.oC_Limit).map(visitOC_Limit))
  }

  override def visitOC_Order(ctx: CypherParser.OC_OrderContext): Order = {
    Order(ctx.oC_SortItem.toNonEmpty(ctx).map(visitOC_SortItem))
  }

  override def visitOC_SortItem(ctx: CypherParser.OC_SortItemContext): SortItem = {
    val maybeSortOrder = if (ctx.ASC || ctx.ASCENDING) {
      Some(Ascending)
    } else if (ctx.DESC || ctx.DESCENDING) {
      Some(Descending)
    } else {
      None
    }
    SortItem(visitOC_Expression(ctx.oC_Expression), maybeSortOrder)
  }

  override def visitOC_Skip(ctx: CypherParser.OC_SkipContext): Skip = {
    Skip(visitOC_Expression(ctx.oC_Expression))
  }

  override def visitOC_Limit(ctx: CypherParser.OC_LimitContext): Limit = {
    Limit(visitOC_Expression(ctx.oC_Expression))
  }

  override def visitOC_ReturnItems(ctx: CypherParser.OC_ReturnItemsContext): ReturnItems = {
    val star = ctx.containsTerminal("*")
    val items = Option(ctx.oC_ReturnItem).map(_.map(visitOC_ReturnItem)).getOrElse(List.empty)
    ReturnItems(star, items)
  }

  override def visitOC_ReturnItem(ctx: CypherParser.OC_ReturnItemContext): ReturnItem = {
    val expression = visitOC_Expression(ctx.oC_Expression)
    if (ctx.AS != null) {
      Alias(expression, visitOC_Variable(ctx.oC_Variable))
    } else {
      expression
    }
  }

  override def visitOC_PropertyKeyName(ctx: CypherParser.OC_PropertyKeyNameContext): PropertyKeyName = {
    PropertyKeyName(ctx.getText)
  }

  override def visitOC_Expression(ctx: CypherParser.OC_ExpressionContext): Expression = {
    visitOC_OrExpression(ctx.oC_OrExpression)
  }

  override def visitOC_OrExpression(ctx: CypherParser.OC_OrExpressionContext): Expression = {
    val ors = ctx.oC_XorExpression.map(visitOC_XorExpression)
    ors match {
      case Nil => throw new IllegalArgumentException("Empty OR")
      case h :: Nil => h
      case _ => OrExpression(NonEmptyList.fromListUnsafe(ors))
    }
  }

  override def visitOC_XorExpression(ctx: CypherParser.OC_XorExpressionContext): Expression = {
    val xors = ctx.oC_AndExpression.map(visitOC_AndExpression)
    xors match {
      case Nil => throw new IllegalArgumentException("Empty XOR")
      case h :: Nil => h
      case _ => XorExpression(NonEmptyList.fromListUnsafe(xors))
    }
  }

  override def visitOC_AndExpression(ctx: CypherParser.OC_AndExpressionContext): Expression = {
    val ands = ctx.oC_NotExpression.map(visitOC_NotExpression)
    ands match {
      case Nil => throw new IllegalArgumentException("Empty AND")
      case h :: Nil => h
      case _ => AndExpression(NonEmptyList.fromListUnsafe(ands))
    }
  }

  override def visitOC_NotExpression(ctx: CypherParser.OC_NotExpressionContext): Expression = {
    val expression = visitOC_ComparisonExpression(ctx.oC_ComparisonExpression)
    if (ctx.NOT.size % 2 != 0) {
      NotExpression(expression)
    } else {
      expression
    }
  }

  override def visitOC_ComparisonExpression(ctx: CypherParser.OC_ComparisonExpressionContext): Expression = {
    val expression = visitOC_AddOrSubtractExpression(ctx.oC_AddOrSubtractExpression)
    val partialComparisonExpressions = ctx.oC_PartialComparisonExpression.asScala.toList
    partialComparisonExpressions.foldLeft(expression) { case (left, partialComparisonExpr) =>
      val op = partialComparisonExpr.children.terminalsLike("<=<>>=").head
      val right = visitOC_AddOrSubtractExpression(partialComparisonExpr.oC_AddOrSubtractExpression)
      op match {
        case "=" => EqualExpression(left, right)
        case "<>" => NotExpression(EqualExpression(left, right))
        case "<" => LessThanExpression(left, right)
        case ">" => NotExpression(LessThanOrEqualExpression(left, right))
        case "<=" => LessThanOrEqualExpression(left, right)
        case ">=" => NotExpression(LessThanExpression(left, right))
      }
    }
  }

  override def visitOC_AddOrSubtractExpression(ctx: CypherParser.OC_AddOrSubtractExpressionContext): Expression = {
    val expressions = ctx.oC_MultiplyDivideModuloExpression.map(visitOC_MultiplyDivideModuloExpression)
    val ops = ctx.children.terminalsLike("+-")
    val opsWithExpr = ops zip expressions.tail
    opsWithExpr.foldLeft(expressions.head) { case (left, (op, right)) =>
      op match {
        case "+" => AddExpression(left, right)
        case "-" => SubtractExpression(left, right)
        case _ => throw new IllegalArgumentException("Unbalanced AddOrSubtractExpression")
      }
    }
  }

  override def visitOC_MultiplyDivideModuloExpression(ctx: CypherParser.OC_MultiplyDivideModuloExpressionContext): Expression = {
    val expressions = ctx.oC_PowerOfExpression.map(visitOC_PowerOfExpression)
    val ops = ctx.children.terminalsLike("*/%")
    val opsWithExpr = ops zip expressions.tail
    opsWithExpr.foldLeft(expressions.head) { case (left, (op, right)) =>
      op match {
        case "*" => MultiplyExpression(left, right)
        case "/" => DivideExpression(left, right)
        case "%" => ModuloExpression(left, right)
        case _ => throw new IllegalArgumentException("Unbalanced MultiplyDivideModuloExpression")
      }
    }
  }

  override def visitOC_PowerOfExpression(ctx: CypherParser.OC_PowerOfExpressionContext): Expression = {
    val expressions = ctx.oC_UnaryAddOrSubtractExpression.map(visitOC_UnaryAddOrSubtractExpression)
    // Right associative => reverse ops and expressions
    val ops = ctx.children.terminalsLike("^").reverse
    val opsWithExpr = ops zip expressions.reverse.tail
    opsWithExpr.foldLeft(expressions.last) { case (exponent, (op, base)) =>
      op match {
        case "^" => PowerOfExpression(base, exponent)
        case other =>
          ctx.show()
          println(s"operator = $other, ops = $ops")
          throw new IllegalArgumentException("Unbalanced MultiplyDivideModuloExpression")
      }
    }
  }

  override def visitOC_UnaryAddOrSubtractExpression(ctx: CypherParser.OC_UnaryAddOrSubtractExpressionContext): Expression = {
    val expr = visitOC_StringListNullOperatorExpression(ctx.oC_StringListNullOperatorExpression)
    val ops = ctx.children.terminalsLike("-")
    if (ops.size % 2 != 0) {
      UnarySubtractExpression(expr)
    } else {
      expr
    }
  }

  override def visitOC_StringListNullOperatorExpression(ctx: CypherParser.OC_StringListNullOperatorExpressionContext): Expression = {
    val propertyOrLabelsExpression = visitOC_PropertyOrLabelsExpression(ctx.oC_PropertyOrLabelsExpression)
    val operatorExpressions = ctx.oC_ListOperatorExpression.map(visitOC_ListOperatorExpression)
    if (operatorExpressions.isEmpty) {
      propertyOrLabelsExpression
    } else {
      StringListNullOperatorExpression(propertyOrLabelsExpression, NonEmptyList.fromListUnsafe(operatorExpressions))
    }
  }

  override def visitOC_PropertyOrLabelsExpression(ctx: CypherParser.OC_PropertyOrLabelsExpressionContext): Expression = {
    val atom = visitOC_Atom(ctx.oC_Atom)
    val propertyLookups = Option(ctx.oC_PropertyLookup).map(_.map(visitOC_PropertyLookup)).getOrElse(List.empty)
    val maybeNodeLabels = Option(ctx.oC_NodeLabels).map(visitOC_NodeLabels)
    if (propertyLookups.isEmpty && maybeNodeLabels.isEmpty) {
      atom
    } else {
      PropertyOrLabelsExpression(atom, propertyLookups, maybeNodeLabels)
    }
  }

  override def visitOC_Atom(ctx: CypherParser.OC_AtomContext): Atom = {
    if (ctx.COUNT) {
      CountStar
    } else if (ctx.FILTER) {
      Filter(visitOC_FilterExpression(ctx.oC_FilterExpression))
    } else if (ctx.EXTRACT) {
      Extract(
        visitOC_FilterExpression(ctx.oC_FilterExpression),
        Option(ctx.oC_Expression).map(visitOC_Expression)
      )
    } else if (ctx.ALL) {
      FilterAll(visitOC_FilterExpression(ctx.oC_FilterExpression))
    } else if (ctx.ANY) {
      FilterAny(visitOC_FilterExpression(ctx.oC_FilterExpression))
    } else if (ctx.NONE) {
      FilterNone(visitOC_FilterExpression(ctx.oC_FilterExpression))
    } else if (ctx.SINGLE) {
      FilterSingle(visitOC_FilterExpression(ctx.oC_FilterExpression))
    } else {
      visitAlternatives[Atom](ctx)
    }
  }

  override def visitOC_Literal(ctx: CypherParser.OC_LiteralContext): Literal = {
    if (ctx.NULL) {
      NullLiteral
    } else if (ctx.StringLiteral) {
      parser.StringLiteral(ctx.StringLiteral.getText)
    } else {
      visitAlternatives[Literal](ctx)
    }
  }

  override def visitOC_NumberLiteral(ctx: CypherParser.OC_NumberLiteralContext): NumberLiteral = {
    // TODO: implement missing alternatives
    visitAlternatives[NumberLiteral](ctx)
  }

  override def visitOC_IntegerLiteral(ctx: CypherParser.OC_IntegerLiteralContext): IntegerLiteral = {
    val value = if (ctx.DecimalInteger) {
      java.lang.Long.parseLong(ctx.getText)
    } else if (ctx.HexInteger) {
      java.lang.Long.parseLong(ctx.getText.drop(2), 16)
    } else {
      java.lang.Long.parseLong(ctx.getText, 8)
    }
    IntegerLiteral(value)
  }

  override def visitChildren(node: RuleNode): CypherAst = {
    throw new RuntimeException(s"Not implemented: ${node.getRuleContext.getClass.getSimpleName.dropRight(7)}")
    null.asInstanceOf[CypherAst]
  }

  override def visitOC_Variable(ctx: CypherParser.OC_VariableContext): Variable = {
    Variable(ctx.getText)
  }

  override def visitOC_Properties(ctx: CypherParser.OC_PropertiesContext): Properties = {
    visitAlternatives[Properties](ctx)
  }

  override def visitOC_MapLiteral(ctx: CypherParser.OC_MapLiteralContext): MapLiteral = {
    val keys = ctx.oC_PropertyKeyName.map(visitOC_PropertyKeyName)
    val values = ctx.oC_Expression.map(visitOC_Expression)
    MapLiteral(keys.zip(values))
  }

  override def visitOC_ListLiteral(ctx: CypherParser.OC_ListLiteralContext): ListLiteral = {
    ListLiteral(ctx.oC_Expression.map(visitOC_Expression))
  }

  override def visitOC_Unwind(ctx: CypherParser.OC_UnwindContext): Unwind = {
    Unwind(visitOC_Expression(ctx.oC_Expression), visitOC_Variable(ctx.oC_Variable))
  }

  // TODO: Why can the return body be null? Grammar suggests otherwise.
  override def visitOC_With(ctx: CypherParser.OC_WithContext): With = {
    With(
      ctx.DISTINCT,
      Option(ctx.oC_ReturnBody)
        .map(visitOC_ReturnBody)
        .getOrElse(ReturnBody.empty),
      Option(ctx.oC_Where).map(visitOC_Where)
    )
  }

  override def visitOC_Set(ctx: CypherParser.OC_SetContext): SetClause = {
    SetClause(ctx.oC_SetItem.toNonEmpty(ctx).map(visitOC_SetItem))
  }

  override def visitOC_SetItem(ctx: CypherParser.OC_SetItemContext): SetItem = {
    val firstChild = ctx.children.get(0)
    firstChild match {
      case pec: OC_PropertyExpressionContext =>
        val pe = visitOC_PropertyExpression(pec)
        val value = visitOC_Expression(ctx.oC_Expression)
        SetProperty(pe, value)
      case vc: OC_VariableContext =>
        val v = visitOC_Variable(vc)
        if (ctx.containsTerminal("=")) {
          SetVariable(v, visitOC_Expression(ctx.oC_Expression))
        } else if (ctx.containsTerminal("+=")) {
          SetAdditionalItem(v, visitOC_Expression(ctx.oC_Expression))
        } else {
          SetLabels(v, visitOC_NodeLabels(ctx.oC_NodeLabels))
        }
      case other => illegalState(ctx, other)
    }
  }

  override def visitOC_PropertyExpression(ctx: CypherParser.OC_PropertyExpressionContext): PropertyExpression = {
    PropertyExpression(visitOC_Atom(ctx.oC_Atom), ctx.oC_PropertyLookup.toNonEmpty(ctx).map(visitOC_PropertyLookup))
  }

  override def visitOC_PropertyLookup(ctx: CypherParser.OC_PropertyLookupContext): PropertyLookup = {
    parser.PropertyKeyName(ctx.oC_PropertyKeyName.getText)
  }

  override def visitOC_NodeLabels(ctx: CypherParser.OC_NodeLabelsContext): NodeLabels = {
    NodeLabels(ctx.oC_NodeLabel.toNonEmpty(ctx).map(visitOC_NodeLabel))
  }

  override def visitOC_NodeLabel(ctx: CypherParser.OC_NodeLabelContext): NodeLabel = {
    NodeLabel(ctx.oC_LabelName.getText)
  }

  override def visitOC_Match(ctx: CypherParser.OC_MatchContext): Match = {
    val optional = ctx.OPTIONAL
    val pattern = visitOC_Pattern(ctx.oC_Pattern)
    val maybeWhere = Option(ctx.oC_Where).map(visitOC_Where)
    Match(optional, pattern, maybeWhere)
  }

  override def visitOC_Where(ctx: CypherParser.OC_WhereContext): Where = {
    Where(visitOC_Expression(ctx.oC_Expression))
  }

  override def visitOC_Pattern(ctx: CypherParser.OC_PatternContext): Pattern = {
    Pattern(ctx.oC_PatternPart.toNonEmpty(ctx).map(visitOC_PatternPart))
  }

  override def visitOC_PatternPart(ctx: CypherParser.OC_PatternPartContext): PatternPart = {
    val patternElement = visitOC_PatternElement(ctx.oC_AnonymousPatternPart.oC_PatternElement)
    val maybeVariable = Option(ctx.oC_Variable).map(visitOC_Variable)
    PatternPart(patternElement, maybeVariable)
  }

  override def visitOC_PatternElement(ctx: CypherParser.OC_PatternElementContext): PatternElement = {
    if (ctx.containsTerminal("\"")) {
      visitOC_PatternElement(ctx.oC_PatternElement)
    } else {
      val nodePattern = visitOC_NodePattern(ctx.oC_NodePattern)
      val patternElementChain = ctx.oC_PatternElementChain.map(visitOC_PatternElementChain)
      PatternElement(nodePattern, patternElementChain)
    }
  }

  override def visitOC_NodePattern(ctx: CypherParser.OC_NodePatternContext): NodePattern = {
    val maybeVariable = Option(ctx.oC_Variable).map(visitOC_Variable)
    val maybeNodeLabels = Option(ctx.oC_NodeLabels).map(visitOC_NodeLabels)
    val maybeProperties = Option(ctx.oC_Properties).map(visitOC_Properties)
    NodePattern(maybeVariable, maybeNodeLabels, maybeProperties)
  }

  override def visitOC_PatternElementChain(ctx: CypherParser.OC_PatternElementChainContext): PatternElementChain = {
    val relationshipPattern = visitOC_RelationshipPattern(ctx.oC_RelationshipPattern)
    val nodePattern = visitOC_NodePattern(ctx.oC_NodePattern)
    PatternElementChain(relationshipPattern, nodePattern)
  }

  override def visitOC_RelationshipPattern(ctx: CypherParser.OC_RelationshipPatternContext): RelationshipPattern = {
    val maybeRelationshipDetail = Option(ctx.oC_RelationshipDetail).map(visitOC_RelationshipDetail)
    if (ctx.oC_LeftArrowHead) {
      if (ctx.oC_RightArrowHead) {
        Undirected(maybeRelationshipDetail)
      } else {
        RightToLeft(maybeRelationshipDetail)
      }
    } else {
      if (ctx.oC_RightArrowHead) {
        LeftToRight(maybeRelationshipDetail)
      } else {
        Undirected(maybeRelationshipDetail)
      }
    }
  }

  override def visitOC_RelationshipDetail(ctx: CypherParser.OC_RelationshipDetailContext): RelationshipDetail = {
    val maybeVariable = Option(ctx.oC_Variable).map(visitOC_Variable)
    val maybeRelationshipTypes = Option(ctx.oC_RelationshipTypes).map(visitOC_RelationshipTypes)
    val maybeRangeLiteral = Option(ctx.oC_RangeLiteral).map(visitOC_RangeLiteral)
    val maybeProperties = Option(ctx.oC_Properties).map(visitOC_Properties)
    RelationshipDetail(maybeVariable, maybeRelationshipTypes, maybeRangeLiteral, maybeProperties)
  }

  override def visitOC_RelationshipTypes(ctx: CypherParser.OC_RelationshipTypesContext): RelationshipTypes = {
    RelationshipTypes(ctx.oC_RelTypeName.toNonEmpty(ctx).map(visitOC_RelTypeName))
  }

  override def visitOC_RelTypeName(ctx: CypherParser.OC_RelTypeNameContext): RelTypeName = {
    RelTypeName(ctx.oC_SchemaName.getText)
  }

  override def visitOC_RangeLiteral(ctx: CypherParser.OC_RangeLiteralContext): RangeLiteral = {
    val fromTo = ctx.oC_IntegerLiteral.map(visitOC_IntegerLiteral)
    val containsDots = ctx.containsTerminal("..")
    fromTo match {
      case List(from, to) => FromToRange(from, to)
      case List(from) if !containsDots => FromRange(from)
      case List(to) if containsDots => ToRange(to)
      case List() => UnboundedRange
      case _ => illegalState(ctx, ctx.oC_IntegerLiteral)
    }
  }

  override def visitOC_FunctionInvocation(ctx: CypherParser.OC_FunctionInvocationContext): FunctionInvocation = {
    val functionName = if (ctx.oC_FunctionName.EXISTS) {
      Exists
    } else {
      visitOC_SymbolicName(ctx.oC_FunctionName.oC_SymbolicName)
    }
    FunctionInvocation(
      functionName,
      ctx.DISTINCT,
      ctx.oC_Expression.map(visitOC_Expression)
    )
  }

  override def visitOC_SymbolicName(ctx: CypherParser.OC_SymbolicNameContext): SymbolicName = {
    SymbolicName(ctx.getText)
  }

  override def visitOC_ListOperatorExpression(ctx: CypherParser.OC_ListOperatorExpressionContext): ListOperatorExpression = {
    val dotDot: Option[Expression] = None
    val parsedChildren: List[Option[Expression]] = ctx.children.asScala.toList.flatMap {
      case c: CypherParser.OC_ExpressionContext =>
        Some(Some(visitOC_Expression(c)))
      case t: TerminalNode if t.getText == ".." => Some(dotDot)
      case _ => None
    }

    parsedChildren match {
      case List(Some(expr)) => SingleElementListOperatorExpression(expr)
      case List(Some(from), `dotDot`, Some(to)) => FromToRangeListOperatorExpression(from, to)
      case List(`dotDot`, Some(to)) => ToRangeListOperatorExpression(to)
      case List(Some(from), `dotDot`) => FromRangeListOperatorExpression(from)
      case List(`dotDot`) => EmptyRangeListOperatorExpression
    }
  }

  override def visitOC_InQueryCall(ctx: CypherParser.OC_InQueryCallContext): InQueryCall = {
    val yieldItems = Option(ctx.oC_YieldItems)
      .map(_.oC_YieldItem.asScala.toList)
      .getOrElse(List.empty[OC_YieldItemContext])
      .map(visitOC_YieldItem)
    InQueryCall(visitOC_ExplicitProcedureInvocation(ctx.oC_ExplicitProcedureInvocation), yieldItems)
  }

  override def visitOC_YieldItem(ctx: CypherParser.OC_YieldItemContext): YieldItem = {
    YieldItem(Option(ctx.oC_ProcedureResultField).map(visitOC_ProcedureResultField), visitOC_Variable(ctx.oC_Variable))
  }

  override def visitOC_ProcedureResultField(ctx: CypherParser.OC_ProcedureResultFieldContext): ProcedureResultField = {
    ProcedureResultField(visitOC_SymbolicName(ctx.oC_SymbolicName))
  }

  override def visitOC_ExplicitProcedureInvocation(ctx: CypherParser.OC_ExplicitProcedureInvocationContext): ExplicitProcedureInvocation = {
    ExplicitProcedureInvocation(visitOC_ProcedureName(ctx.oC_ProcedureName), ctx.oC_Expression.map(visitOC_Expression))
  }

  override def visitOC_ImplicitProcedureInvocation(ctx: CypherParser.OC_ImplicitProcedureInvocationContext): ImplicitProcedureInvocation = {
    ImplicitProcedureInvocation(visitOC_ProcedureName(ctx.oC_ProcedureName))
  }

  override def visitOC_ProcedureName(ctx: CypherParser.OC_ProcedureNameContext): ProcedureName = {
    ProcedureName(visitOC_Namespace(ctx.oC_Namespace), visitOC_SymbolicName(ctx.oC_SymbolicName))
  }

  override def visitOC_Namespace(ctx: CypherParser.OC_NamespaceContext): Namespace = {
    Namespace(ctx.oC_SymbolicName.map(visitOC_SymbolicName))
  }

  override def visitOC_PatternComprehension(ctx: CypherParser.OC_PatternComprehensionContext): PatternComprehension = {
    val expressions = ctx.oC_Expression.map(visitOC_Expression)
    val (maybeWhereExpression, expression) = expressions match {
      case List(e) => None -> e
      case List(we, e) => Some(we) -> e
      case _ => illegalState(ctx, ctx.oC_Expression)
    }
    PatternComprehension(
      Option(ctx.oC_Variable).map(visitOC_Variable),
      visitOC_RelationshipsPattern(ctx.oC_RelationshipsPattern),
      expression,
      maybeWhereExpression
    )
  }

  override def visitOC_RelationshipsPattern(ctx: CypherParser.OC_RelationshipsPatternContext): RelationshipsPattern = {
    val patternElementChains = ctx.oC_PatternElementChain.toNonEmpty(ctx).map(visitOC_PatternElementChain)
    RelationshipsPattern(visitOC_NodePattern(ctx.oC_NodePattern), patternElementChains)
  }

  override def visitOC_MergeAction(ctx: CypherParser.OC_MergeActionContext): MergeAction = {
    visitAlternatives[MergeAction](ctx)
  }

  override def visitOC_Merge(ctx: CypherParser.OC_MergeContext): Merge = {
    Merge(visitOC_PatternPart(ctx.oC_PatternPart), ctx.oC_MergeAction.map(visitOC_MergeAction))
  }

  override def visitOC_Create(ctx: CypherParser.OC_CreateContext): Create = {
    Create(visitOC_Pattern(ctx.oC_Pattern))
  }

  override def visitOC_Remove(ctx: CypherParser.OC_RemoveContext): Remove = {
    Remove(ctx.oC_RemoveItem.toNonEmpty(ctx).map(visitOC_RemoveItem))
  }

  override def visitOC_RemoveItem(ctx: CypherParser.OC_RemoveItemContext): RemoveItem = {
    if (ctx.oC_Variable) {
      val nodeLabels = ctx.oC_NodeLabels.oC_NodeLabel.toNonEmpty(ctx).map(visitOC_NodeLabel)
      RemoveNodeVariable(visitOC_Variable(ctx.oC_Variable), nodeLabels)
    } else {
      visitOC_PropertyExpression(ctx.oC_PropertyExpression)
    }
  }

  override def visitOC_ParenthesizedExpression(ctx: CypherParser.OC_ParenthesizedExpressionContext): ParenthesizedExpression = {
    ParenthesizedExpression(visitOC_Expression(ctx.oC_Expression))
  }

  override def visitOC_BooleanLiteral(ctx: CypherParser.OC_BooleanLiteralContext): BooleanLiteral = {
    BooleanLiteral(ctx.getText.toBoolean)
  }

  override def visitOC_DoubleLiteral(ctx: CypherParser.OC_DoubleLiteralContext): DoubleLiteral = {
    DoubleLiteral(ctx.getText.toDouble)
  }

  override def visitOC_Parameter(ctx: CypherParser.OC_ParameterContext): Parameter = {
    if (ctx.oC_SymbolicName) {
      visitOC_SymbolicName(ctx.oC_SymbolicName)
    } else {
      IndexParameter(ctx.DecimalInteger.getText.toLong)
    }
  }

  override def visitOC_StandaloneCall(ctx: CypherParser.OC_StandaloneCallContext): StandaloneCall = {
    val procedureInvocation = Option(ctx.oC_ExplicitProcedureInvocation)
      .map(visitOC_ExplicitProcedureInvocation)
      .getOrElse(visitOC_ImplicitProcedureInvocation(ctx.oC_ImplicitProcedureInvocation))
    val yieldItems = Option(ctx.oC_YieldItems)
      .map(_.oC_YieldItem)
      .map(_.map(visitOC_YieldItem))
      .getOrElse(List.empty[YieldItem])
    StandaloneCall(procedureInvocation, yieldItems)
  }

  override def visitOC_ListComprehension(ctx: CypherParser.OC_ListComprehensionContext): ListComprehension = {
    ListComprehension(
      visitOC_FilterExpression(ctx.oC_FilterExpression),
      Option(ctx.oC_Expression).map(visitOC_Expression)
    )
  }

  override def visitOC_FilterExpression(ctx: CypherParser.OC_FilterExpressionContext): FilterExpression = {
    FilterExpression(visitOC_IdInColl(ctx.oC_IdInColl), Option(ctx.oC_Where).map(visitOC_Where))
  }

  override def visitOC_IdInColl(ctx: CypherParser.OC_IdInCollContext): IdInColl = {
    IdInColl(visitOC_Variable(ctx.oC_Variable), visitOC_Expression(ctx.oC_Expression))
  }


  //  override def visitOC_HexInteger(ctx: CypherParser.OC_HexIntegerContext): String = {
  //    HexInteger("ctx.getText", (visitOC_HexDigit(ctx.oC_HexDigit)).rep(min = 1))
  //  }
  //  override def visitOC_CaseAlternatives(ctx: CypherParser.OC_CaseAlternativesContext): CaseAlternatives = {
  //    CaseAlternatives(ctx.getText, (visitOC_SP(ctx.oC_SP)).?, visitOC_Expression(ctx.oC_Expression), (visitOC_SP(ctx.oC_SP)).?, ctx.getText, (visitOC_SP(ctx.oC_SP)).?, visitOC_Expression(ctx.oC_Expression))
  //  }
  //  override def visitOC_EscapedSymbolicName(ctx: CypherParser.OC_EscapedSymbolicNameContext): String = {
  //    EscapedSymbolicName((ctx.getText, (ctx.getText).rep, ctx.getText).rep(min = 1))
  //  }
  //  override def visitOC_Variable(ctx: CypherParser.OC_VariableContext): Variable = {
  //    Variable(visitOC_SymbolicName(ctx.oC_SymbolicName))
  //  }
  //  override def visitOC_ReservedWord(ctx: CypherParser.OC_ReservedWordContext): ReservedWord = {
  //    visitChildren(ctx).asInstanceOf[ReservedWord]
  //  }
  //  override def visitOC_RightArrowHead(ctx: CypherParser.OC_RightArrowHeadContext): String = {
  //    RightArrowHead(ctx.getText)
  //  }
  //  override def visitOC_DecimalInteger(ctx: CypherParser.OC_DecimalIntegerContext): String = {
  //    DecimalInteger(((ctx.getText) | (visitOC_ZeroDigit(ctx.oC_ZeroDigit)) | (visitOC_NonZeroDigit(ctx.oC_NonZeroDigit), (visitOC_Digit(ctx.oC_Digit)).rep)))
  //  }
  //  override def visitOC_FunctionName(ctx: CypherParser.OC_FunctionNameContext): FunctionName = {
  //    visitChildren(ctx).asInstanceOf[FunctionName]
  //  }
  //  override def visitOC_SP(ctx: CypherParser.OC_SPContext): Unit = {
  //    SP((visitOC_whitespace(ctx.oC_whitespace)).rep(min = 1))
  //  }
  //  override def visitOC_NonZeroDigit(ctx: CypherParser.OC_NonZeroDigitContext): String = {
  //    NonZeroDigit(((ctx.getText) | (visitOC_NonZeroOctDigit(ctx.oC_NonZeroOctDigit))))
  //  }
  //  override def visitOC_UpdatingStartClause(ctx: CypherParser.OC_UpdatingStartClauseContext): UpdatingStartClause = {
  //    visitChildren(ctx).asInstanceOf[UpdatingStartClause]
  //  }
  //  override def visitOC_AnonymousPatternPart(ctx: CypherParser.OC_AnonymousPatternPartContext): AnonymousPatternPart = {
  //    visitChildren(ctx).asInstanceOf[AnonymousPatternPart]
  //  }
  //  override def visitOC_Query(ctx: CypherParser.OC_QueryContext): Query = {
  //    visitChildren(ctx).asInstanceOf[Query]
  //  }
  //  override def visitOC_LabelName(ctx: CypherParser.OC_LabelNameContext): LabelName = {
  //    LabelName(visitOC_SchemaName(ctx.oC_SchemaName))
  //  }
  //  override def visitOC_ReadUpdateEnd(ctx: CypherParser.OC_ReadUpdateEndContext): ReadUpdateEnd = {
  //    ReadUpdateEnd(visitOC_ReadingClause(ctx.oC_ReadingClause), ((visitOC_SP(ctx.oC_SP)).?, visitOC_ReadingClause(ctx.oC_ReadingClause)).rep, ((visitOC_SP(ctx.oC_SP)).?, visitOC_UpdatingClause(ctx.oC_UpdatingClause)).rep(min = 1), ((visitOC_SP(ctx.oC_SP)).?, visitOC_Return(ctx.oC_Return)).?)
  //  }
  //  override def visitOC_RegularDecimalReal(ctx: CypherParser.OC_RegularDecimalRealContext): String = {
  //    RegularDecimalReal((visitOC_Digit(ctx.oC_Digit)).rep, ctx.getText, (visitOC_Digit(ctx.oC_Digit)).rep(min = 1))
  //  }
  //  override def visitOC_UpdatingEnd(ctx: CypherParser.OC_UpdatingEndContext): UpdatingEnd = {
  //    UpdatingEnd(visitOC_UpdatingStartClause(ctx.oC_UpdatingStartClause), ((visitOC_SP(ctx.oC_SP)).?, visitOC_UpdatingClause(ctx.oC_UpdatingClause)).rep, ((visitOC_SP(ctx.oC_SP)).?, visitOC_Return(ctx.oC_Return)).?)
  //  }
  //  override def visitOC_NonZeroOctDigit(ctx: CypherParser.OC_NonZeroOctDigitContext): String = {
  //    NonZeroOctDigit(ctx.getText)
  //  }
  //  override def visitOC_LeftArrowHead(ctx: CypherParser.OC_LeftArrowHeadContext): String = {
  //    LeftArrowHead(ctx.getText)
  //  }
  //  override def visitOC_OctalInteger(ctx: CypherParser.OC_OctalIntegerContext): String = {
  //    OctalInteger(visitOC_ZeroDigit(ctx.oC_ZeroDigit), (visitOC_OctDigit(ctx.oC_OctDigit)).rep(min = 1))
  //  }
  //  override def visitOC_HexDigit(ctx: CypherParser.OC_HexDigitContext): String = {
  //    HexDigit(((ctx.getText) | (visitOC_Digit(ctx.oC_Digit)) | (visitOC_HexLetter(ctx.oC_HexLetter))))
  //  }
  //  override def visitOC_SchemaName(ctx: CypherParser.OC_SchemaNameContext): SchemaName = {
  //    visitChildren(ctx).asInstanceOf[SchemaName]
  //  }
  //  override def visitOC_IdentifierStart(ctx: CypherParser.OC_IdentifierStartContext): String = {
  //    IdentifierStart(ctx.getText)
  //  }
  //  override def visitOC_YieldItems(ctx: CypherParser.OC_YieldItemsContext): YieldItems = {
  //    visitChildren(ctx).asInstanceOf[YieldItems]
  //  }
  //  override def visitOC_ZeroDigit(ctx: CypherParser.OC_ZeroDigitContext): String = {
  //    ZeroDigit(ctx.getText)
  //  }
  //  override def visitOC_Digit(ctx: CypherParser.OC_DigitContext): String = {
  //    Digit(((ctx.getText) | (visitOC_ZeroDigit(ctx.oC_ZeroDigit)) | (visitOC_NonZeroDigit(ctx.oC_NonZeroDigit))))
  //  }
  //  override def visitOC_OctDigit(ctx: CypherParser.OC_OctDigitContext): String = {
  //    OctDigit(((ctx.getText) | (visitOC_ZeroDigit(ctx.oC_ZeroDigit)) | (visitOC_NonZeroOctDigit(ctx.oC_NonZeroOctDigit))))
  //  }
  //  override def visitOC_Dash(ctx: CypherParser.OC_DashContext): String = {
  //    Dash(ctx.getText)
  //  }
  //  override def visitOC_HexLetter(ctx: CypherParser.OC_HexLetterContext): String = {
  //    HexLetter(ctx.getText)
  //  }
  //  override def visitOC_whitespace(ctx: CypherParser.OC_whitespaceContext): Unit = {
  //    whitespace(((ctx.getText) | (visitOC_Comment(ctx.oC_Comment))))
  //  }
  //  override def visitOC_CaseExpression(ctx: CypherParser.OC_CaseExpressionContext): CaseExpression = {
  //    CaseExpression(((ctx.getText) | (ctx.getText, ((visitOC_SP(ctx.oC_SP)).?, visitOC_CaseAlternatives(ctx.oC_CaseAlternatives)).rep(min = 1)) | (ctx.getText, (visitOC_SP(ctx.oC_SP)).?, visitOC_Expression(ctx.oC_Expression), ((visitOC_SP(ctx.oC_SP)).?, visitOC_CaseAlternatives(ctx.oC_CaseAlternatives)).rep(min = 1))), ((visitOC_SP(ctx.oC_SP)).?, ctx.getText, (visitOC_SP(ctx.oC_SP)).?, visitOC_Expression(ctx.oC_Expression)).?, (visitOC_SP(ctx.oC_SP)).?, ctx.getText)
  //  }
  //  override def visitOC_EscapedChar(ctx: CypherParser.OC_EscapedCharContext): String = {
  //    EscapedChar(ctx.getText, ((ctx.getText) | (ctx.getText, (visitOC_HexDigit(ctx.oC_HexDigit)).rep(min = 4, max = 4)) | (ctx.getText, (visitOC_HexDigit(ctx.oC_HexDigit)).rep(min = 8, max = 8))))
  //  }
  //  override def visitOC_Match(ctx: CypherParser.OC_MatchContext): Match = {
  //    Match((ctx.getText, visitOC_SP(ctx.oC_SP)).?, ctx.getText, (visitOC_SP(ctx.oC_SP)).?, visitOC_Pattern(ctx.oC_Pattern), ((visitOC_SP(ctx.oC_SP)).?, visitOC_Where(ctx.oC_Where)).?)
  //  }
  //  override def visitOC_Comment(ctx: CypherParser.OC_CommentContext): String = {
  //    Comment(((ctx.getText) | (ctx.getText, (((ctx.getText) | (ctx.getText, ctx.getText))).rep, ctx.getText) | (ctx.getText, (ctx.getText).rep, (ctx.getText).?, ctx.getText)))
  //  }
  //  override def visitOC_IdentifierPart(ctx: CypherParser.OC_IdentifierPartContext): String = {
  //    IdentifierPart(ctx.getText)
  //  }
  //  override def visitOC_UpdatingPart(ctx: CypherParser.OC_UpdatingPartContext): UpdatingPart = {
  //    UpdatingPart((visitOC_UpdatingClause(ctx.oC_UpdatingClause), (visitOC_SP(ctx.oC_SP)).?).rep)
  //  }


}
