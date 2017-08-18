package org.opencypher.caps.impl.instances.spark

import org.opencypher.caps.api.ir.global.Label
import org.opencypher.caps.api.value.CypherValue
import org.opencypher.caps.api.value.CypherValueUtils._
import org.opencypher.caps.impl.convert.toJavaType
import org.opencypher.caps.impl.exception.Raise

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

  def in[T](elem: Any, list: Any): Boolean = list match {
    case a: mutable.WrappedArray[_] => a.contains(elem)
    case x => Raise.invalidArgument("an array", x.toString)
  }

}
