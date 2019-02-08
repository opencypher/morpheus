package org.opencypher.okapi.api.io.conversion

import org.opencypher.okapi.api.types.CTPattern

object PatternMapping {

  def from(nodeMapping: NodeMapping, relationshipMapping: RelationshipMapping): PatternMapping = {
    PatternMapping(nodeMapping, relationshipMapping.copy(sourceStartNodeKey = ""))
  }
}

final case class PatternMapping protected(
  nodeMapping: NodeMapping,
  relationshipMapping: RelationshipMapping
) extends EntityMapping {

  override val cypherType = CTPattern(nodeMapping.cypherType, relationshipMapping.cypherType)

  override def sourceIdKey: String = ??? //nodeMapping.sourceIdKey

  override val idKeys: Seq[String] = (nodeMapping.idKeys ++ relationshipMapping.idKeys).distinct

  override def propertyMapping: Map[String, String] = ??? //nodeMapping.propertyMapping ++ relationshipMapping.propertyMapping

}
