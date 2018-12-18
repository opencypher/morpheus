package org.opencypher.spark.impl.util

import org.apache.spark.sql.{Column, DataFrame, functions}
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherString}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr.{Expr, Param}
import org.opencypher.okapi.relational.impl.table.RecordHeader

object TemporalTypesHelper {

  def sanitize(arg: Expr)(implicit header: RecordHeader, df: DataFrame, parameters: CypherMap): Column = {
    arg match {
      case Param(name) => {
        val s = parameters(name) match {
          case CypherString(s) => s
          case other => throw IllegalArgumentException("a CypherString", other)
        }
        val sanitizedTemporalString = s.split("T").toList match {
          case head :: Nil => sanitizeDate(head)
          case head :: tail => {
            val date = sanitizeDate(head)
            assert(tail.size == 1, "The character `T` should only appear once in a temporal type string.")
            val timeAndTimezone = tail.head.split("[Z+-]").toList match {
              case head :: Nil => sanitizeTime(head)
              case head :: tail => {
                val time = sanitizeTime(head)
                assert(tail.size == 1, "The characters `Z`, `+`, `-`, should only appear once in a temporal type string.")
                time + sanitizeTimezone(tail.head)
              }
              case Nil => ""
            }
            date + timeAndTimezone
          }
          case Nil => ""
        }
        functions.lit(sanitizedTemporalString)
      }

      case other => ???
    }
  }

  private def sanitizeDate(date: String): String = "date"

  private def sanitizeTime(time: String): String = "Ttime"

  private def sanitizeTimezone(timezone: String): String = "Ztimezone"

}
