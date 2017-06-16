package org.opencypher.spark.api.schema

import org.opencypher.spark.api.types._
import org.opencypher.spark.api.util.{Verifiable, Verified}

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
}

object PropertyKeyMap {
  val empty = PropertyKeyMap(Map.empty)()
}

final case class PropertyKeyMap(m: Map[String, Map[String, CypherType]])(val conflicts: Set[String] = Set.empty) {
  def keysFor(classifier: String): Map[String, CypherType] = m.getOrElse(classifier, Map.empty)
  def withKeys(classifier: String, keys: Seq[(String, CypherType)]): PropertyKeyMap = {
    val oldKeys = m.getOrElse(classifier, Map.empty)
    val newKeys = keys.toMap
    val newConflicts = oldKeys.collect {
      case (k, t) =>
        newKeys.get(k) match {
          case Some(otherT) if t != otherT =>
            Some(s"Conflicting schema for '$classifier'! Key '$k' has type $t but also has type ${newKeys(k)}")
          case _ =>
            None
        }
    }.flatten.toSet
    copy(m.updated(classifier, oldKeys ++ newKeys))(conflicts = conflicts ++ newConflicts)
  }

  def keys = m.values.flatMap(_.keySet).toSet

  def ++(other: PropertyKeyMap) = copy(m ++ other.m)(conflicts ++ other.conflicts)
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

  def ++(other: ImpliedLabels) = copy(m ++ other.m)

  private def implicationsFor(source: String) = m.getOrElse(source, Set.empty) + source
}

case class OptionalLabels(combos: Set[Set[String]]) {

  def combinationsFor(label: String): Set[String] = combos.find(_(label)).getOrElse(Set.empty)

  def withCombination(a: String, b: String): OptionalLabels = {
    val (lhs, rhs) = combos.partition(labels => labels(a) || labels(b))
    copy(combos = rhs + (lhs.flatten + a + b))
  }

  def ++(other: OptionalLabels) = copy(combos ++ other.combos)
}

final case class Schema(
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
  optionalLabels: OptionalLabels) extends Verifiable {

  self: Schema =>

  override type Self = Schema
  override type VerifiedSelf = VerifiedSchema

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

  def keys = nodeKeyMap.keys ++ relKeyMap.keys

  lazy val conflictSet = nodeKeyMap.conflicts ++ relKeyMap.conflicts

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

  def ++(other: Schema) = {
    copy(labels ++ other.labels, relationshipTypes ++ other.relationshipTypes, nodeKeyMap ++ other.nodeKeyMap, relKeyMap ++ other.relKeyMap, impliedLabels ++ other.impliedLabels, optionalLabels ++ other.optionalLabels)
  }

  override def verify: VerifiedSchema = {
    // TODO:
    //
    // We envision this to change in two ways
    // (1) Only enforce correct types for a property key between implied labels
    // (2) Use union types (and generally support them) for combined labels
    //

    if (conflictSet.nonEmpty) {
      conflictSet.foreach(println)
      throw new IllegalStateException(s"Schema invalid: $self")
    }

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

  override def toString = {
    val builder = new StringBuilder

    labels.foreach { label =>
      builder.append(s":$label\n")
      nodeKeys(label).foreach {
        case (key, typ) => builder.append(s"\t$key: $typ\n")
      }
    }

    relationshipTypes.foreach { relType =>
      builder.append(s":$relType\n")
      relationshipKeys(relType).foreach {
        case (key, typ) => builder.append(s"\t$key: $typ\n")
      }
    }

    builder.toString
  }
}

sealed trait VerifiedSchema extends Verified[Schema] {
  final override def v = schema
  def schema: Schema
}
