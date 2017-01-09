package org.opencypher.spark.api.schema

import org.opencypher.spark.api.CypherType

object StdSchema {
  val empty: Schema = StdSchema(
    labels = Set.empty,
    relationshipTypes = Set.empty,
    nodeKeyMap = PropertyKeyMap.empty,
    relKeyMap = PropertyKeyMap.empty,
    labelImplications = LabelImplications(Map.empty),
    labelCombinations = LabelCombinations(Set.empty)
  )
}

object PropertyKeyMap {
  val empty = PropertyKeyMap(Map.empty)
}

case class PropertyKeyMap(m: Map[String, Map[String, CypherType]]) {
  def keysFor(classifier: String): Map[String, CypherType] = m.getOrElse(classifier, Map.empty)
  def withKeys(classifier: String, keys: Seq[(String, CypherType)]): PropertyKeyMap =
    copy(m.updated(classifier, keys.toMap))
}

case class LabelImplications(m: Map[String, Set[String]]) {

  def transitiveImplicationsFor(known: Set[String]): Set[String] = {
    val next = known.flatMap(implicationsFor)
    if (next == known) known else transitiveImplicationsFor(next)
  }

  def withImplication(source: String, target: String): LabelImplications = {
    val implied = implicationsFor(source)
    if (implied(target)) this else copy(m = m.updated(source, implied + target))
  }

  private def implicationsFor(source: String) = m.getOrElse(source, Set.empty) + source
}

case class LabelCombinations(combos: Set[Set[String]]) {

  def combinationsFor(label: String): Set[String] = combos.find(_(label)).getOrElse(Set.empty)

  def withCombination(a: String, b: String): LabelCombinations = {
    val (lhs, rhs) = combos.partition(labels => labels(a) || labels(b))
    copy(combos = rhs + (lhs.flatten + a + b))
  }
}

case class StdSchema(labels: Set[String], relationshipTypes: Set[String],
                     nodeKeyMap: PropertyKeyMap, relKeyMap: PropertyKeyMap,
                     labelImplications: LabelImplications, labelCombinations: LabelCombinations) extends Schema {
  override def impliedLabels(knownLabels: Set[String]): Set[String] =
    labelImplications.transitiveImplicationsFor(knownLabels.intersect(labels))

  override def combinedLabels(knownLabels: Set[String]): Set[String] = knownLabels.flatMap(labelCombinations.combinationsFor)

  override def nodeKeys(label: String): Map[String, CypherType] = nodeKeyMap.keysFor(label)

  override def relationshipKeys(typ: String): Map[String, CypherType] = relKeyMap.keysFor(typ)

  override def withImpliedLabel(existingLabel: String, impliedLabel: String): Schema =
    copy(labels = labels + existingLabel + impliedLabel,
      labelImplications = labelImplications.withImplication(existingLabel, impliedLabel))

  override def withCombinedLabels(a: String, b: String): Schema =
    copy(labels = labels + a + b, labelCombinations = labelCombinations.withCombination(a, b))

  override def withNodeKeys(label: String)(keys: (String, CypherType)*): Schema =
    copy(labels = labels + label, nodeKeyMap = nodeKeyMap.withKeys(label, keys))

  override def withRelationshipKeys(typ: String)(keys: (String, CypherType)*): Schema =
    copy(relationshipTypes = relationshipTypes + typ, relKeyMap = relKeyMap.withKeys(typ, keys))
}

trait Schema {

  /**
    * All labels present in this graph
    */
  def labels: Set[String]

  /**
    * All relationship types present in this graph
    */
  def relationshipTypes: Set[String]

  /**
    * Given a set of labels that a node definitely has, returns all labels the node _must_ have.
    */
  def impliedLabels(knownLabels: Set[String]): Set[String]

  /**
    * Given a set of labels that a node definitely has, returns all the labels that the node could possibly have.
    */
  def combinedLabels(knownLabels: Set[String]): Set[String]

  /**
    * Given a set of labels that a node definitely has, returns its property schema.
    */
  def nodeKeys(label: String): Map[String, CypherType]

  /**
    * Returns the property schema for a given relationship type
    */
  def relationshipKeys(typ: String): Map[String, CypherType]

  def withImpliedLabel(existingLabel: String, impliedLabel: String): Schema

  def withCombinedLabels(existingLabel: String, possibleLabel: String): Schema

  def withNodeKeys(label: String)(keys: (String, CypherType)*): Schema

  def withRelationshipKeys(typ: String)(keys: (String, CypherType)*): Schema
}

