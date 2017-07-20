/**
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
package org.opencypher.spark.impl.spark

import org.apache.spark.sql.{Column, DataFrame}
import org.opencypher.spark.api.expr.Property
import org.opencypher.spark.api.record.{FieldSlotContent, ProjectedExpr, RecordSlot, SlotContent}

import scala.collection.mutable

object SparkColumn {
  def from(df: DataFrame): RecordSlot => Column = (record) => df.col(SparkColumnName.of(record.content))
}

object SparkColumnName {

  def of(slot: RecordSlot): String = of(slot.content)

  def of(slot: SlotContent): String = {
    val builder = slot match {
      case fieldContent: FieldSlotContent => new NameBuilder() += fieldContent.field.name
      case ProjectedExpr(p: Property) => new NameBuilder() += None += p.withoutType + p.cypherType.material.name
      case ProjectedExpr(expr) => new NameBuilder() += None += expr.withoutType
    }

    builder.result()
  }

  def from(parts: Option[String]*): String =
    parts.foldLeft(new NameBuilder()) { case (builder, part) => builder += part }.result()

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
