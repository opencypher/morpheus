package org.opencypher.spark.prototype.api.record

import org.opencypher.spark.api.CypherType
import org.opencypher.spark.prototype.api.expr.Expr

final case class RecordSlot(name: String, exprs: Set[Expr], cypherType: CypherType)
