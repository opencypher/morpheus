/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.okapi.relational.impl.table

import java.util.UUID

import org.opencypher.okapi.ir.api.expr.{Expr, Property, Var}

import scala.collection.mutable

object ColumnName {

  def tempColName: String =
    UUID.randomUUID().toString

  def of(slot: RecordSlot): String = of(slot.content)

  def of(slot: SlotContent): String = {
    val builder = slot match {
      case fieldContent: FieldSlotContent => new NameBuilder() += fieldContent.field.name
      case ProjectedExpr(p: Property) => new NameBuilder() += None += p.withoutType + p.cypherType.material.name
      case ProjectedExpr(expr) => new NameBuilder() += None += expr.withoutType
    }

    builder.result()
  }

  def of(expr: Expr): String = {
    expr match {
      case Var(name) => name
      case _: Property =>
        val columnNameBuilder = new NameBuilder() += None += expr.withoutType + expr.cypherType.material.name
        columnNameBuilder.result()
      case _ =>
        val columnNameBuilder = new NameBuilder() += None += expr.withoutType
        columnNameBuilder.result()
    }
  }

  def from(name: String): String = (new NameBuilder() += name).result()

  final class NameBuilder(sizeHint: Int = 16) extends mutable.Builder[Option[String], String] {

    private val builder = new StringBuilder()
    builder.sizeHint(sizeHint)

    override def +=(part: Option[String]): this.type = part match {
      case None => builder.append("__"); this
      case Some(text) => this += text
    }

    def +=(part: String): this.type = {
      if (builder.nonEmpty)
        builder.append("__")

      if (part.isEmpty)
        builder.append("_empty_")
      else {
        val ch0 = part.charAt(0)
        if (isValidIdentStart(ch0)) {
          builder.append(ch0)
        } else {
          if (Character.isDigit(ch0))
            builder.append('_')
          addEscapedUnlessValidPart(builder, ch0)
        }

        part
          .substring(1)
          .replaceAllLiterally("<-", "_left_arrow_")
          .replaceAllLiterally("->", "_right_arrow_")
          .replaceAllLiterally("--", "_double_dash_")
          .foreach(addEscapedUnlessValidPart(builder, _))
      }

      this
    }

    override def clear(): Unit = {
      builder.clear()
    }

    override def result(): String =
      builder.result()

    private def addEscapedUnlessValidPart(builder: StringBuilder, ch: Char): Unit =
      if (isValidIdentPart(ch)) builder.append(ch) else builder.append(escapeChar(ch))

    private def isValidIdentStart(ch: Char) =
      Character.isLetter(ch)

    private def isValidIdentPart(ch: Char) =
      Character.isLetterOrDigit(ch)

    private def escapeChar(ch: Char) = ch match {
      case ' ' => "_space_"
      case '_' => "_bar_"
      case '.' => "_dot_"
      case ',' => "_comma_"
      case '#' => "_hash_"
      case '%' => "_percent_"
      case '@' => "_at_"
      case '&' => "_amp_"
      case '|' => "_pipe_"
      case '^' => "_caret_"
      case '$' => "_dollar_"
      case '?' => "_query_"
      case '!' => "_exclamation_"
      case ':' => ":"
      case ';' => "_semicolon_"
      case '-' => "_dash_"
      case '+' => "_plus_"
      case '*' => "_star_"
      case '/' => "_slash_"
      case '\\' => "_backslash_"
      case '\'' => "_single_quote_"
      case '`' => "_backquote_"
      case '"' => "_double_quote_"
      case '(' => "("
      case '[' => "_open_bracket_"
      case '{' => "_open_brace_"
      case ')' => ")"
      case ']' => "_close_bracket_"
      case '}' => "_close_brace_"
      case '<' => "_lt_"
      case '>' => "_gt_"
      case '=' => "_eq_"
      case '~' => "_tilde_"
      case '§' => "_section_"
      case '°' => "_deg_"
      case '\r' => "_cr_"
      case '\n' => "_nl_"
      case '\t' => "_tab_"
      case '\f' => "_ff_"
      case '\b' => "_backspace_"
      case _ => s"_u${Integer.toHexString(ch.toInt)}_"
    }
  }

}
