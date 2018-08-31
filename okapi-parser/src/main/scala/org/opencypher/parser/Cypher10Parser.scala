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

import org.antlr.v4.runtime._
import org.antlr.v4.runtime.tree.{ParseTree, TerminalNode, TerminalNodeImpl}
import org.opencypher.okapi.api.exception.CypherException
import org.opencypher.okapi.api.exception.CypherException.ErrorPhase.CompileTime
import org.opencypher.okapi.api.exception.CypherException.ErrorType.SyntaxError
import org.opencypher.okapi.api.exception.CypherException._
import org.opencypher.okapi.api.value.CypherValue._

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.Try

object Cypher10Parser {

  def parse(query: String, parameters: Map[String, CypherValue] = Map.empty): CypherAst = {
    val input = CharStreams.fromString(query)
    val lexer = new CypherLexer(input)
    lexer.removeErrorListeners()
    lexer.addErrorListener(AntlrErrorListerner(query))
    val tokens = new CommonTokenStream(lexer)
    val parser = new CypherParser(tokens)
    parser.removeErrorListeners()
    parser.addErrorListener(AntlrErrorListerner(query))
    val tree = parser.oC_Cypher
    val ast = AntlrAstTransformer.visit(tree)
    ast
  }

}

case class AntlrException(
  override val errorType: ErrorType,
  override val phase: ErrorPhase,
  override val detail: ErrorDetails
)
  extends CypherException(errorType, phase, detail)

case class AntlrErrorListerner(string: String) extends BaseErrorListener {

  override def syntaxError(
    recognizer: Recognizer[_, _],
    offendingSymbol: scala.Any,
    lineNumber: Int,
    charPositionInLine: Int,
    msg: String,
    e: RecognitionException
  ): Unit = {
    val line = string.split("\\r?\\n")
    val offendingLine = line(lineNumber - 1)
    val ruleNames = recognizer.getRuleNames
    val vocabulary = recognizer.getVocabulary
    val maybeContext = Option(e)
      .flatMap(ex => Option(ex.getCtx))
    val maybeRuleName = maybeContext
      .flatMap(c => Option(c.getRuleIndex))
      .map(ruleNames)
    val maybeFailingRuleString = maybeRuleName
      .map(rn => s"\nFailing rule: $rn")
    val maybeExpectedTokens = Try(e.getExpectedTokens).toOption
      .map(_.toList.asScala.map(i => vocabulary.getDisplayName(i)))
    val maybeExpectedTokensString = maybeExpectedTokens
      .map(ts => s"Expected tokens: ${ts.mkString(", ")}")
    val maybeAntlrExceptionString = Option(e)
      .map(ae => s"AntlrException: $ae")
    val maybeOffendingSymbol = Option(offendingSymbol)
    val maybeOffendingSymbolString = maybeOffendingSymbol.map(s => s"Offending symbol: $s")

    val msg =
      s"""|on line $lineNumber, character $charPositionInLine:
          |\t$offendingLine
          |\t${"~" * charPositionInLine}^${"~" * (offendingLine.length - charPositionInLine)}
          |${
        Seq(
          maybeOffendingSymbolString,
          maybeFailingRuleString,
          maybeExpectedTokensString,
          maybeAntlrExceptionString
        ).flatten.mkString("\n")
      }""".stripMargin

    @tailrec
    def recLastChild(tree: ParseTree): ParseTree = {
      if (tree.getChildCount == 0) {
        tree
      } else {
        recLastChild(tree.getChild(tree.getChildCount - 1))
      }
    }

    val maybeLastChild: Option[ParseTree] = maybeContext match {
      case Some(c) => Some(recLastChild(c))
      case None =>
        recognizer match {
          case c: CypherParser => Some(recLastChild(c.getContext))
          case _: CypherLexer => Option.empty[ParseTree]
        }
    }

    val maybeLastSuccessName = maybeLastChild.map {
      case t: TerminalNode => vocabulary.getDisplayName(t.getSymbol.getType)
      case p => p.getClass.getSimpleName
    }

    val detail = if (maybeRuleName.contains("oC_RelationshipPattern")) {
      InvalidRelationshipPattern(msg)
    } else if (maybeLastSuccessName.contains("HexInteger")) {
      InvalidNumberLiteral(msg)
    } else {
      ParsingError(msg)
    }

    throw AntlrException(SyntaxError, CompileTime, detail)
  }

}
