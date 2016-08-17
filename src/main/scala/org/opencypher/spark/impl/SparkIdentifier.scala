package org.opencypher.spark.impl

object SparkIdentifier {

  val empty = new SparkIdentifier("")

  def apply(name: String): SparkIdentifier = {
    if (name.isEmpty)
      SparkIdentifier.empty
    else {
      val builder = new StringBuilder()
      builder.sizeHint(name.length + 16)
      val ch0 = name.charAt(0)
      if (isValidIdentStart(ch0)) builder.append(ch0) else builder.append(escapeChar(ch0))

      name
        .substring(1)
        .replaceAllLiterally("<-", "_left_arrow_")
        .replaceAllLiterally("->", "_right_arrow_")
        .replaceAllLiterally("--", "_double_dash_")
        .foreach { ch =>
          if (isValidIdentPart(ch)) builder.append(ch) else builder.append(escapeChar(ch))
        }

      new SparkIdentifier(builder.result())
    }
  }

  private def isValidIdentStart(ch: Char) =
    Character.isLetter(ch) || ch == '_'

  private def isValidIdentPart(ch: Char) =
    Character.isLetterOrDigit(ch) || ch == '_'

  private def escapeChar(ch: Char) = ch match {
    case '_' => "__"
    case '.' => "_dot_"
    case ',' => "_comma_"
    case '#' => "_hash_"
    case '%' => "_percent_"
    case '@' => "_at_"
    case '&' => "_amp_"
    case '|' => "_pipe_"
    case '^' => "_caret_"
    case '$' => "_str_"
    case '?' => "_question_"
    case '!' => "_exclamation_"
    case ':' => "_colon_"
    case ';' => "_semicolon_"
    case '-' => "_dash_"
    case '+' => "_plus_"
    case '*' => "_star_"
    case '/' => "_slash_"
    case '\\' => "_backslash_"
    case '\'' => "_tick_"
    case '`' => "_backtick_"
    case '"' => "_quote_"
    case '(' => "_open_paren_"
    case '[' => "_open_bracket_"
    case '{' => "_open_brace_"
    case '<' => "_open_angle_"
    case ')' => "_close_paren_"
    case ']' => "_close_bracket_"
    case '}' => "_close_brace_"
    case '>' => "_close_angle_"
    case '\r' => "_cr_"
    case '\n' => "_nl_"
    case '\t' => "_tab_"
    case '\f' => "_ff_"
    case _ if Character.isDigit(ch) => s"_$ch"
    case _ => s"_u${Integer.toHexString(ch.toInt)}_"
  }
}

final class SparkIdentifier private[SparkIdentifier](val name: String) extends AnyVal {
  def ++(other: SparkIdentifier) = new SparkIdentifier(s"$name${other.name}")
  def symbol = Symbol(name)
  override def toString = s"SparkIdentifier($name)"
}
