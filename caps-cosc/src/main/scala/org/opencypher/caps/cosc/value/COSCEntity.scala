package org.opencypher.caps.cosc.value

import org.opencypher.caps.api.value.CypherValue.{CypherMap, CypherNode, CypherRelationship}

case class COSCNode(
  override val id: Long,
  override val labels: Set[String] = Set.empty,
  override val properties: CypherMap = CypherMap.empty) extends CypherNode[Long](id, labels, properties)

case class COSCRelationship(
  override val id: Long,
  override val source: Long,
  override val target: Long,
  override val relType: String,
  override val properties: CypherMap = CypherMap.empty) extends CypherRelationship(id, source, target, relType, properties)
