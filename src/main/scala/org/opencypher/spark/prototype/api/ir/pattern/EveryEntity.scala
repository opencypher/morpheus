package org.opencypher.spark.prototype.api.ir.pattern

import org.opencypher.spark.prototype.api.ir.global.{LabelRef, RelTypeRef}

sealed trait EveryEntity
sealed case class EveryNode(labels: AllGiven[LabelRef]) extends EveryEntity
object EveryNode extends EveryNode(AllGiven[LabelRef]())

sealed case class EveryRelationship(relTypes: AnyGiven[RelTypeRef]) extends EveryEntity
object EveryRelationship extends EveryRelationship(AnyGiven[RelTypeRef]())

