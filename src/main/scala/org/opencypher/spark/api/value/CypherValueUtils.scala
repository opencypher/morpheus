package org.opencypher.spark.api.value

object CypherValueUtils {
  implicit class RichCypherValue(value: CypherValue) {

    def <(rhs: CypherValue): Option[Boolean] = {
      CypherValue.comparability(value, rhs).map(_ < 0)
    }

    def <=(rhs: CypherValue): Option[Boolean] = {
      CypherValue.comparability(value, rhs).map(_ <= 0)
    }

    def >(rhs: CypherValue): Option[Boolean] = {
      CypherValue.comparability(value, rhs).map(_ > 0)
    }

    def >=(rhs: CypherValue): Option[Boolean] = {
      CypherValue.comparability(value, rhs).map(_ >= 0)
    }
  }
}
