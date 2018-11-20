package org.opencypher.memcypher.impl.types

import org.opencypher.memcypher.impl.types.CypherTypeOps._
import org.opencypher.okapi.api.types.CypherType._
import org.opencypher.okapi.api.types.{CTFloat, CTInteger, CTNumber}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherBoolean, CypherFloat, CypherInteger, CypherValue}
import org.opencypher.okapi.impl.exception.IllegalArgumentException

object CypherValueOps {

  implicit class ToCypherValue(val value: Any) extends AnyVal {
    def toCypherValue: CypherValue = CypherValue(value)
  }

  implicit class RichCypherValue(val value: CypherValue) extends AnyVal {

    def unary_! : Boolean =
      !value.asInstanceOf[CypherBoolean].unwrap

    def &&(other: CypherValue): CypherValue = {
      value.asInstanceOf[CypherBoolean].unwrap && other.asInstanceOf[CypherBoolean].unwrap
    }

    def ||(other: CypherValue): CypherValue = {
      value.asInstanceOf[CypherBoolean].unwrap || other.asInstanceOf[CypherBoolean].unwrap
    }

    def ==(other: CypherValue): Boolean = {
      value.cypherType.join(other.cypherType).equivalence.asInstanceOf[Equiv[Any]].equiv(value.unwrap, other.unwrap)
    }

    def !=(other: CypherValue): Boolean = {
      !value.cypherType.join(other.cypherType).equivalence.asInstanceOf[Equiv[Any]].equiv(value.unwrap, other.unwrap)
    }

    def >(other: CypherValue): Boolean = {
      value.cypherType.join(other.cypherType).ordering.asInstanceOf[Ordering[Any]].gt(value.unwrap, other.unwrap)
    }

    def >=(other: CypherValue): Boolean = {
      value.cypherType.join(other.cypherType).ordering.asInstanceOf[Ordering[Any]].gteq(value.unwrap, other.unwrap)
    }

    def <(other: CypherValue): Boolean = {
      value.cypherType.join(other.cypherType).ordering.asInstanceOf[Ordering[Any]].lt(value.unwrap, other.unwrap)
    }

    def <=(other: CypherValue): Boolean = {
      value.cypherType.join(other.cypherType).ordering.asInstanceOf[Ordering[Any]].lteq(value.unwrap, other.unwrap)
    }

    def +(other: CypherValue): CypherValue = {
      if (value.cypherType.sameTypeAs(other.cypherType).isTrue && value.cypherType.subTypeOf(CTNumber).isTrue) {
        value.cypherType match {
          case CTInteger => CypherInteger(value.as[Long].get + other.as[Long].get)
          case CTFloat => CypherFloat(value.as[Double].get + other.as[Double].get)
          case _ => throw IllegalArgumentException("CypherInteger or CypherFloat", other)
        }
      } else {
        throw IllegalArgumentException("Identical number types", s"${value.cypherType} and ${other.cypherType}")
      }
    }
  }
}
