package org.opencypher.caps.impl.spark

import org.apache.spark.sql.catalyst.expressions.ArrayContains
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{ArrayType, StringType}
import org.apache.spark.sql.{Column, functions}

object CAPSFunctions {

  /**
    * Alternative version of `array_contains` that takes a column as the value.
    */
  def array_contains(column: Column, value: Column): Column =
    new Column(ArrayContains(column.expr, value.expr))

  def array_append_long(array: Column, value: Column): Column = {
    appendLongUDF(array, value)
  }

  private val appendLongUDF = functions.udf(appendLong _)

  private def appendLong(array: Seq[Long], element: Long): Seq[Long] = {
    array :+ element
  }

  def get_node_labels(labelNames: Seq[String]): UserDefinedFunction = {
    functions.udf(filterWithMask(labelNames) _, ArrayType(StringType, containsNull = false))
  }

  private def filterWithMask(dataToFilter: Seq[String])(mask: Seq[Boolean]): Seq[String] = {
    dataToFilter.zip(mask).collect {
      case (label, true) => label
    }
  }

  def get_property_keys(propertyKeys: Seq[String]): UserDefinedFunction = {
    functions.udf(filterNotNull(propertyKeys) _, ArrayType(StringType, containsNull = false))
  }

  private def filterNotNull(dataToFilter: Seq[String])(values: Seq[Any]): Seq[String] = {
    dataToFilter.zip(values).collect {
      case (key, value) if value != null => key
    }
  }

}
