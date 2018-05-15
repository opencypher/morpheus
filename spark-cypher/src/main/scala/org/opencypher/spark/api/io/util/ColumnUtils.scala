package org.opencypher.spark.api.io.util

import scala.annotation.tailrec

object ColumnUtils {

  val propertyPrefix: String = "property#"

  implicit class StringConversion(val s: String) extends AnyVal {

    def toPropertyColumnName: String = {
      s"$propertyPrefix${s.encodeToSQLCompatible}"
    }

    def isPropertyColumnName: Boolean = s.startsWith(propertyPrefix)

    def toProperty: String = {
      if (s.isPropertyColumnName) {
        s.drop(propertyPrefix.length).decodeFromSQLCompatible
      } else {
        s
      }
    }

    def encodeToSQLCompatible: String = {
      s.flatMap {
        case c if c.isLetterOrDigit => Seq(c)
        case u@'_' => Seq(u)
        case h@'#' => Seq(h)
        case special: Char =>
          "@" + special.toHexString.padOnLeft(4)
      }.mkString
    }

    def decodeFromSQLCompatible: String = {
      val sb = new StringBuilder
      @tailrec def recDecode(remaining: String): Unit = {
        if (remaining.nonEmpty) {
          remaining.head match {
            case '@' =>
              val hexString = remaining.tail.take(4)
              sb.append(hexString.parseHex)
              recDecode(remaining.drop(5))
            case other =>
              sb.append(other)
              recDecode(remaining.tail)
          }
        }
      }
      recDecode(s)
      sb.toString
    }

    def parseHex: Char = Integer.parseInt(s, 16).toChar

    def padOnLeft(chars: Int, char: Char = '0'): String = {
      s.reverse.padTo(chars, char).reverse
    }

  }

}
