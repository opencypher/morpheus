package org.opencypher.spark.impl.instances.spark

import org.opencypher.spark.api.ir.global.Label
import org.opencypher.spark.api.value.CypherValue
import org.opencypher.spark.api.value.CypherValueUtils._
import org.opencypher.spark.impl.convert.toJavaType

import scala.collection.mutable

object Udfs {

  def const(v: CypherValue): () => Any = () => toJavaType(v)

  // TODO: Try to share code with cypherFilter()
  def lt(lhs: Any, rhs: Any): Any = (CypherValue(lhs) < CypherValue(rhs)).orNull

  def lteq(lhs: Any, rhs: Any): Any = (CypherValue(lhs) <= CypherValue(rhs)).orNull

  def gteq(lhs: Any, rhs: Any): Any = (CypherValue(lhs) >= CypherValue(rhs)).orNull

  def gt(lhs: Any, rhs: Any): Any = (CypherValue(lhs) > CypherValue(rhs)).orNull

  def getNodeLabels(labelNames: Seq[Label]): (Any) => Array[String] = {
    case a: mutable.WrappedArray[Boolean] =>
      a.zip(labelNames).collect {
        case (true, label) => label.name
      }.toArray
  }

}
