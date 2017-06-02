package org.opencypher.spark.api.value

import org.opencypher.spark.TestSuiteImpl
import org.opencypher.spark.api.types.{False, Maybe, Ternary, True}

import scala.annotation.tailrec

class CypherValueTestSuite extends TestSuiteImpl with CypherValue.Conversion {

  @tailrec
  final def isPathLike(l: Seq[Any], nextIsNode: Ternary = Maybe): Boolean = l match {
    case Seq(_: CypherNode, tail@_*) if nextIsNode.maybeTrue => isPathLike(tail, False)
    case Seq(_: CypherRelationship, tail@_*) if nextIsNode.maybeFalse => isPathLike(tail, True)
    case Seq() => nextIsNode.isDefinite
    case _ => false
  }
}

