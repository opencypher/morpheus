package org.opencypher.spark.impl.record

import org.opencypher.spark.api.ir.global._
import org.opencypher.spark.api.spark.SparkCypherTokens

final case class SparkCypherRecordsTokens(registry: TokenRegistry) extends SparkCypherTokens {

  override type Tokens = SparkCypherRecordsTokens

  override def labels: Set[String] = registry.labels.elts.map(_.name).toSet
  override def relTypes: Set[String] = registry.relTypes.elts.map(_.name).toSet

  override def labelName(id: Int): String = registry.label(LabelRef(id)).name
  override def labelId(name: String): Int = registry.labelRefByName(name).id

  override def relTypeName(id: Int): String = registry.relType(RelTypeRef(id)).name
  override def relTypeId(name: String): Int = registry.relTypeRefByName(name).id

  override def withLabel(name: String) = copy(registry = registry.withLabel(Label(name)))
  override def withRelType(name: String) = copy(registry = registry.withRelType(RelType(name)))
}
