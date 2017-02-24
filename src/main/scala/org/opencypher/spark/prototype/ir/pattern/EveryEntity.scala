package org.opencypher.spark.prototype.ir.pattern

import org.opencypher.spark.prototype.ir.global.{LabelRef, RelTypeRef}

sealed trait EveryEntity
final case class EveryNode(labels: AllGiven[LabelRef] = AllGiven[LabelRef]()) extends EveryEntity
final case class EveryRelationship(relTypes: AnyGiven[RelTypeRef] = AnyGiven[RelTypeRef]()) extends EveryEntity
