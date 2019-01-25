package org.opencypher.okapi.api.io.conversion

import org.opencypher.okapi.api.types.CTPattern

object PatternMapping {

  def apply(nodeMapping: NodeMapping, relationshipMapping: RelationshipMapping): PatternMapping = {
    PatternMapping(nodeMapping, relationshipMapping.copy(sourceStartNodeKey = nodeMapping.sourceIdKey))
  }
}

final case class PatternMapping(
  nodeMapping: NodeMapping,
  relationshipMapping: RelationshipMapping
) extends EntityMapping {

  override val cypherType = CTPattern(nodeMapping.cypherType, relationshipMapping.cypherType)

  override val sourceIdKey: String = nodeMapping.sourceIdKey

  override val idKeys: Seq[String] = nodeMapping.idKeys ++ relationshipMapping.idKeys

  override val propertyMapping: Map[String, String] = nodeMapping.propertyMapping ++ relationshipMapping.propertyMapping
}
