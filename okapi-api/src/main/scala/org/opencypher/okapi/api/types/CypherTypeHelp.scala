package org.opencypher.okapi.api.types

import cats.Monoid
import org.opencypher.okapi.api.types.CypherType.fromName
import upickle.default.{readwriter, _}

object CypherTypeHelp {
  implicit val typeRw: ReadWriter[CypherType] = readwriter[String].bimap[CypherType](_.name, s => fromName(s).get)

  implicit val joinMonoid: Monoid[CypherType] = new Monoid[CypherType] {
    override def empty: CypherType = CTVoid

    override def combine(x: CypherType, y: CypherType): CypherType = x | y
  }
}
