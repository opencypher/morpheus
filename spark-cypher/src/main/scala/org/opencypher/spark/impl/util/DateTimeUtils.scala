package org.opencypher.spark.impl.util

import org.apache.spark.sql.DataFrame
import org.opencypher.okapi.api.value.CypherValue.{CypherFloat, CypherInteger, CypherMap, CypherString}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr.{MapExpression, Param}
import org.opencypher.okapi.relational.impl.table.RecordHeader

object DateTimeUtils {

  def fromMapExpr(map: MapExpression)(implicit header: RecordHeader, df: DataFrame, parameters: CypherMap): String = {
    val calendarIdentifiers = Map("year" -> 4, "month" -> 2, "day" -> 2)
    val timeIdentifiers = Map("hour" -> 2, "minute" -> 2, "second" -> 2)
    val identifiers = calendarIdentifiers ++ timeIdentifiers

    val convertedMapElements = map.items.map(m => m._1 -> (m._2 match {
      case Param(name) => parameters(name) match {
        case CypherFloat(d) => d.toString
        case CypherInteger(l) => l.toString
        case CypherString(s) => s
        case other => throw IllegalArgumentException("a parameter of type CypherFloat, CypherInteger or CypherString", other)
      }
      case other => throw IllegalArgumentException("a param", other)
    }))

    val withMissingElements = identifiers.map {
      case (key, size) =>
        key -> convertedMapElements.getOrElse(key, "")
    }

    val (dates, times) = withMissingElements.partition {
      case (key, value) if calendarIdentifiers.contains(key) => true
      case (key, value) if timeIdentifiers.contains(key) => false
      case other => throw IllegalArgumentException(s"a valid datetime key (one of ${(calendarIdentifiers ++ timeIdentifiers).keys.mkString("[", ",", "]")}", other._1)
    }

    val datesWithAdjustedLengths = dates.map {
      case (key, value) => adjustLengths(identifiers, key, value, "1")
    }

    val timesWithAdjustedLengths = times.map {
      case (key, value) => adjustLengths(identifiers, key, value, "0")
    }

    ???
  }

  private def adjustLengths(identifiers: Map[String, Int], key: String, value: String, minValue: String): (String, String) = {
    if (value.length == 0) key -> s"${"0" * (identifiers(key)-1)}$minValue"
    else key -> s"${"0" * (identifiers(key) - value.length)}$value"
  }

}
