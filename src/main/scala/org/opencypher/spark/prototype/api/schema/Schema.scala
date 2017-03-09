package org.opencypher.spark.prototype.api.schema

import org.opencypher.spark.api.CypherType

import scala.language.implicitConversions

object Schema {
  val empty: Schema = Schema(
    labels = Set.empty,
    relationshipTypes = Set.empty,
    nodeKeyMap = PropertyKeyMap.empty,
    relKeyMap = PropertyKeyMap.empty,
    impliedLabels = ImpliedLabels(Map.empty),
    optionalLabels = OptionalLabels(Set.empty)
  )

  implicit def verifySchema(schema: Schema): VerifiedSchema = schema.verify
}

object PropertyKeyMap {
  val empty = PropertyKeyMap(Map.empty)
}

case class PropertyKeyMap(m: Map[String, Map[String, CypherType]]) {
  def keysFor(classifier: String): Map[String, CypherType] = m.getOrElse(classifier, Map.empty)
  def withKeys(classifier: String, keys: Seq[(String, CypherType)]): PropertyKeyMap =
    copy(m.updated(classifier, keys.toMap))
}

case class ImpliedLabels(m: Map[String, Set[String]]) {

  def transitiveImplicationsFor(known: Set[String]): Set[String] = {
    val next = known.flatMap(implicationsFor)
    if (next == known) known else transitiveImplicationsFor(next)
  }

  def withImplication(source: String, target: String): ImpliedLabels = {
    val implied = implicationsFor(source)
    if (implied(target)) this else copy(m = m.updated(source, implied + target))
  }

  private def implicationsFor(source: String) = m.getOrElse(source, Set.empty) + source
}

case class OptionalLabels(combos: Set[Set[String]]) {

  def combinationsFor(label: String): Set[String] = combos.find(_(label)).getOrElse(Set.empty)

  def withCombination(a: String, b: String): OptionalLabels = {
    val (lhs, rhs) = combos.partition(labels => labels(a) || labels(b))
    copy(combos = rhs + (lhs.flatten + a + b))
  }
}

case class Schema(
  /**
   * All labels present in this graph
   */
  labels: Set[String],
  /**
   * All relationship types present in this graph
   */
  relationshipTypes: Set[String],
  nodeKeyMap: PropertyKeyMap,
  relKeyMap: PropertyKeyMap,
  impliedLabels: ImpliedLabels,
  optionalLabels: OptionalLabels) {

  self: Schema =>

  /**
   * Given a set of labels that a node definitely has, returns all labels the node _must_ have.
   */
  def impliedLabels(knownLabels: Set[String]): Set[String] =
    impliedLabels.transitiveImplicationsFor(knownLabels.intersect(labels))

  /**
   * Given a set of labels that a node definitely has, returns all the labels that the node could possibly have.
   */
  def optionalLabels(knownLabels: Set[String]): Set[String] = knownLabels.flatMap(optionalLabels.combinationsFor)

  /**
   * Given a label that a node definitely has, returns its property schema.
   */
  def nodeKeys(label: String): Map[String, CypherType] = nodeKeyMap.keysFor(label)

  /**
   * Returns the property schema for a given relationship type
   */
  def relationshipKeys(typ: String): Map[String, CypherType] = relKeyMap.keysFor(typ)

  def withImpliedLabel(existingLabel: String, impliedLabel: String): Schema =
    copy(labels = labels + existingLabel + impliedLabel,
      impliedLabels = impliedLabels.withImplication(existingLabel, impliedLabel))

  def withCombinedLabels(a: String, b: String): Schema =
    copy(labels = labels + a + b, optionalLabels = optionalLabels.withCombination(a, b))

  def withNodeKeys(label: String)(keys: (String, CypherType)*): Schema =
    copy(labels = labels + label, nodeKeyMap = nodeKeyMap.withKeys(label, keys))

  def withRelationshipKeys(typ: String)(keys: (String, CypherType)*): Schema =
    copy(relationshipTypes = relationshipTypes + typ, relKeyMap = relKeyMap.withKeys(typ, keys))

  def verify: VerifiedSchema = {
    // TODO:
    //
    // We envision this to change in two ways
    // (1) Only enforce correct types for a property key between implied labels
    // (2) Use union types (and generally support them) for combined labels
    //

    val coOccurringLabels =
      for (
        label <- labels;
        other <- impliedLabels(Set(label)) ++ optionalLabels(Set(label))
      ) yield label -> other

    for ((label, other) <- coOccurringLabels) {
      val xKeys = nodeKeys(label)
      val yKeys = nodeKeys(other)
      if (xKeys.keySet.intersect(yKeys.keySet).exists(key => xKeys(key) != yKeys(key)))
        throw new IllegalArgumentException(s"Failed to verify schema for labels :$label and :$other")
    }

    new VerifiedSchema {
      override def schema = self
    }
  }
}

sealed trait VerifiedSchema {
  def schema: Schema
}
