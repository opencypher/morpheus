package org.opencypher.spark.api.ir.pattern

import org.opencypher.spark.api.ir.global.{Label, RelType}

sealed trait EveryEntity
sealed case class EveryNode(labels: AllGiven[Label]) extends EveryEntity
object EveryNode extends EveryNode(AllGiven[Label]())

sealed case class EveryRelationship(relTypes: AnyGiven[RelType]) extends EveryEntity
object EveryRelationship extends EveryRelationship(AnyGiven[RelType]())

