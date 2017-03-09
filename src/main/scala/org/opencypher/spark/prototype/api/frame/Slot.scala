package org.opencypher.spark.prototype.api.frame

import org.opencypher.spark.api.CypherType
import org.opencypher.spark.prototype.api.expr.Expr
import org.relaxng.datatype.Datatype

final case class Slot(cypher: (Set[Expr], CypherType), spark: (Symbol, Datatype))
