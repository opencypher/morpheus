package org.opencypher.spark.api.value

object CypherValueUtils {
  implicit class RichCypherValue(value: CypherValue) {

    def <(rhs: CypherValue): Option[Boolean] = {
      CypherValue.comparability(value, rhs) match {
        case None => None
        case Some(x) if x < 0 => Some(true)
        case _ => Some(false)
      }
    }

    def <=(rhs: CypherValue): Option[Boolean] = {
      CypherValue.comparability(value, rhs) match {
        case None => None
        case Some(x) if x < 0 || x == 0 => Some(true)
        case _ => Some(false)
      }
    }

    def >(rhs: CypherValue): Option[Boolean] = {
      CypherValue.comparability(value, rhs) match {
        case None => None
        case Some(x) if x > 0 => Some(true)
        case _ => Some(false)
      }
    }
  }
}
