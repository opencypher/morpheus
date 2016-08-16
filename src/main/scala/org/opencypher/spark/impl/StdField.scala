package org.opencypher.spark.impl

import org.opencypher.spark.api.{CypherField, CypherType}

object StdField {
  def apply(pair: (Symbol, CypherType)): StdField = new StdField(pair._1, pair._2)
}

final case class StdField(sym: Symbol, cypherType: CypherType) extends CypherField {
  def column = SparkIdentifier(sym.name)
}

object SparkIdentifier {

  def apply(name: String): SparkIdentifier = {
    val builder = new StringBuilder(name.length + 16)
    name.foreach {
      case ch if isUnescapedChar(ch) => builder.append(ch)
      case '-' => builder.append("--")
      case '#' => builder.append("-hash-")
      case '@' => builder.append("-at-")
      case '`' => builder.append("-backtick-")
      case ';' => builder.append("-semicolon-")
      case '\'' => builder.append("-tick-")
      case '\"' => builder.append("-quote-")
      case '/' => builder.append("-slash-")
      case '\\' => builder.append("-backslash-")
      case '\t' => builder.append("-tab-")
      case '\n' => builder.append("-nl-")
      case '\r' => builder.append("-ff-")
      case ch => builder.append(s"-U+${Integer.toHexString(ch.toInt)}-")
    }
    SparkIdentifier(builder.result())
  }

  def isEscapedIdentifierName(name: String) =
    name.length > 0 && Character.isLetter(name.charAt(0)) && name.forall(isRegularChar)

  private def isUnescapedChar(ch: Char) =
    Character.isLetterOrDigit(ch) || "()[]<>.:+*/|&$".contains(ch)

  private def isRegularChar(ch: Char) =
    Character.isLetterOrDigit(ch) || ch == '-'
}

final class SparkIdentifier private[SparkIdentifier](val name: String) extends AnyVal {
  def ++(other: SparkIdentifier) = new SparkIdentifier(s"$name-$other")
  def symbol = Symbol(if (SparkIdentifier.isEscapedIdentifierName(name)) name else s"`$name`")
}
