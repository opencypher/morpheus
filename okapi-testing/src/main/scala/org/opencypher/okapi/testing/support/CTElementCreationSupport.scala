package org.opencypher.okapi.testing.support

import org.opencypher.okapi.api.types.{CTNode, CTRelationship, CTString}

object CTElementCreationSupport {

  private val properties: Map[String, CTString.type] = Map("key" -> CTString)

  def getRelationship: CTRelationship = getRelationship("REL_TYPE")
  def getRelationship(relTypes: String*): CTRelationship = CTRelationship.fromAlternatives(relTypes.toSet, properties)

  def getNode: CTNode = getNode("Label")
  def getNode(labels: String*): CTNode = CTNode.fromCombo(labels.toSet, properties)
}
