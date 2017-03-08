package org.opencypher.spark.prototype.frame

import org.opencypher.spark.api.CypherType
import org.opencypher.spark.prototype.Expr
import org.relaxng.datatype.Datatype

final case class Slot(cypher: (Expr, CypherType), spark: (Symbol, Datatype))
