package org.opencypher.spark.prototype.ir.pattern

import org.opencypher.spark.prototype.ir.token.{LabelRef, RelTypeRef}

sealed trait AnyEntity
final case class AnyNode(labels: WithEvery[LabelRef] = WithEvery.empty[LabelRef]) extends AnyEntity
final case class AnyRelationship(relTypes: WithAny[RelTypeRef] = WithAny.empty[RelTypeRef]) extends AnyEntity
