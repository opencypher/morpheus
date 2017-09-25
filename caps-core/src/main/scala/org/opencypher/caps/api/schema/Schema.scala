/*
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencypher.caps.api.schema

import org.opencypher.caps.api.types._
import org.opencypher.caps.common.{Verifiable, Verified}
import org.opencypher.caps.impl.spark.exception.Raise
import org.opencypher.caps.ir.api.IRField

import scala.language.implicitConversions

object Schema {
  val empty: Schema = Schema(
    labels = Set.empty,
    relationshipTypes = Set.empty,
    nodeKeyMap = PropertyKeyMap.empty,
    relKeyMap = PropertyKeyMap.empty,
    impliedLabels = ImpliedLabels(Map.empty),
    labelCombinations = LabelCombinations(Set.empty)
  )
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
  /**
    * Property keys associated with a node label
    */
  nodeKeyMap: PropertyKeyMap,
  /**
    * Property keys associated with a relationship type
    */
  relKeyMap: PropertyKeyMap,
  /**
    * Implied labels for each existing label
    */
  impliedLabels: ImpliedLabels,
  /**
    * Groups of labels where each group contains possible label combinations.
    */
  labelCombinations: LabelCombinations) extends Verifiable {

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
  def labelCombination(knownLabels: Set[String]): Set[String] =
    knownLabels.flatMap(labelCombinations.combinationsFor)

  /**
   * Given a label that a node definitely has, returns its property schema.
    *
    * TODO: consider implied labels here?
   */
  def nodeKeys(label: String): Map[String, CypherType] = nodeKeyMap.keysFor(label)

  def keys: Set[String] = nodeKeyMap.keys ++ relKeyMap.keys

  lazy val conflictSet: Set[String] = nodeKeyMap.conflicts ++ relKeyMap.conflicts

  /**
   * Returns the property schema for a given relationship type
   */
  def relationshipKeys(typ: String): Map[String, CypherType] = relKeyMap.keysFor(typ)

  def withImpliedLabel(pair: (String, String)): Schema = withImpliedLabel(pair._1, pair._2)

  def withImpliedLabel(existingLabel: String, impliedLabel: String): Schema =
    copy(labels = labels + existingLabel + impliedLabel,
      impliedLabels = impliedLabels.withImplication(existingLabel, impliedLabel))

  def withLabelCombination(pair: (String, String)): Schema = withLabelCombination(pair._1, pair._2)

  def withLabelCombination(as: String*): Schema =
    copy(labels = labels ++ as, labelCombinations = labelCombinations.withCombinations(as: _*))

  /**
    * Adds information about a label and its associated properties to the schema.
    * The arguments provided to this method are interpreted as describing a whole piece of information,
    * meaning that for a specific instance of the label, the given properties were present in their exact
    * given shape. For example, consider
    *
    * {{{
    *   val s = schema.withNodePropertyKeys("Foo")("p" -> CTString, "q" -> CTInteger)
    *   val t = s.withNodePropertyKeys("Foo")("p" -> CTString)
    * }}}
    *
    * The resulting schema (assigned to `t`) will indicate that the type of `q` is CTInteger.nullable,
    * as the schema understands that it is possible to map `:Foo` to both sets of properties, and it
    * calculates the join of the property types, respectively.
    *
    * @param label the label to add to the schema
    * @param keys the properties (name and type) to associate with the label
    * @return a copy of the Schema with the provided new data
    */
  def withNodePropertyKeys(label: String)(keys: (String, CypherType)*): Schema = {
    if (labels contains label) {
      val updatedTypes = computePropertyTypes(nodeKeyMap.keysFor(label), keys.toMap)

      copy(nodeKeyMap = nodeKeyMap.withKeys(label, updatedTypes))
    } else {
      copy(labels = labels + label, nodeKeyMap = nodeKeyMap.withKeys(label, keys))
    }
  }

  /**
    * Adds information about a relationship type and its associated properties to the schema.
    * The arguments provided to this method are interpreted as describing a whole piece of information,
    * meaning that for a specific instance of the relationship type, the given properties were present
    * in their exact given shape. For example, consider
    *
    * {{{
    *   val s = schema.withRelationshipPropertyKeys("FOO")("p" -> CTString, "q" -> CTInteger)
    *   val t = s.withRelationshipPropertyKeys("FOO")("p" -> CTString)
    * }}}
    *
    * The resulting schema (assigned to `t`) will indicate that the type of `q` is CTInteger.nullable,
    * as the schema understands that it is possible to map `:FOO` to both sets of properties, and it
    * calculates the join of the property types, respectively.
    *
    * @param typ the relationship type to add to the schema
    * @param keys the properties (name and type) to associate with the relationship type
    * @return a copy of the Schema with the provided new data
    */
  def withRelationshipPropertyKeys(typ: String)(keys: (String, CypherType)*): Schema = {
    if (relationshipTypes contains typ) {
      val updatedTypes = computePropertyTypes(relKeyMap.keysFor(typ), keys.toMap)

      copy(relKeyMap = relKeyMap.withKeys(typ, updatedTypes))
    } else {
      copy(relationshipTypes = relationshipTypes + typ, relKeyMap = relKeyMap.withKeys(typ, keys))
    }
  }

  def withRelationshipType(relType: String): Schema =
    copy(relationshipTypes = relationshipTypes + relType)

  private def computePropertyTypes(existing: Map[String, CypherType], input: Map[String, CypherType]) = {
    // Map over input keys to calculate join of type with existing type
    val keysWithJoinedTypes = input.map {
      case (key, propType) =>
        // if the key did not previously exist it's optional (nullable)
        key -> propType.join(existing.getOrElse(key, CTNull))
    }

    // Map over the rest of the existing keys to mark them all nullable
    val propertiesMarkedOptional = existing.filterKeys(k => !input.contains(k)).foldLeft(keysWithJoinedTypes) {
      case (map, (key, propTyp)) =>
        map.updated(key, propTyp.nullable)
    }

    propertiesMarkedOptional.toSeq
  }

  def isEmpty: Boolean = this == Schema.empty

  def ++(other: Schema): Schema = {
    val newLabels = labels ++ other.labels
    val newRelTypes = relationshipTypes ++ other.relationshipTypes
    val newNodeKeyMap = nodeKeyMap ++ other.nodeKeyMap
    val newRelKeyMap = relKeyMap ++ other.relKeyMap
    val newImpliedLabels = inferImpliedLabels(other)

    // new optional labels are previous optional labels and all revoked implied labels
    val combinedOptionalLabels = this.labelCombinations ++ other.labelCombinations
    val newLabelCombinationPairs = this.impliedLabels.toPairs ++ other.impliedLabels.toPairs -- newImpliedLabels.toPairs
    val newLabelCombinations = newLabelCombinationPairs.foldLeft(combinedOptionalLabels)((o,p) => o.withCombinations(p._1, p._2))

    copy(newLabels,
      newRelTypes,
      newNodeKeyMap,
      newRelKeyMap,
      newImpliedLabels,
      newLabelCombinations)
  }

  def forEntities(entities: Set[IRField]): Schema = {
    entities
      .map(entitySchema)
      .foldLeft(Schema.empty)(_ ++ _)
  }

  private def entitySchema(entity: IRField): Schema = entity.cypherType match {
    case n: CTNode =>
      forNode(n)
    case r: CTRelationship =>
      forRelationship(r)
    case x =>
      Raise.invalidArgument("an entity type", x.toString)
  }

  /**
    * Returns the sub-schema for `nodeType`
    *
    * @param nodeType Specifies the type for which the schema is extracted
    * @return sub-schema for `nodeType`
    */
  def forNode(nodeType: CTNode): Schema = {
    val requiredLabels = {
      val explicitLabels = nodeType.labels
      val impliedLabels = this.impliedLabels.transitiveImplicationsFor(explicitLabels)
      explicitLabels union impliedLabels
    }

    val possibleLabels = if (nodeType.labels.isEmpty) {
      this.labels
    } else {
      requiredLabels union this.labelCombinations.filterByLabels(requiredLabels).combos.flatten
    }

    copy(
      labels = possibleLabels,
      Set.empty,
      nodeKeyMap = this.nodeKeyMap.filterByClassifier(possibleLabels),
      relKeyMap = PropertyKeyMap.empty,
      impliedLabels = this.impliedLabels.filterByLabels(possibleLabels),
      labelCombinations = this.labelCombinations.filterByLabels(possibleLabels)
    )
  }

  /**
    * Returns the sub-schema for `relType`
    *
    * @param relType Specifies the type for which the schema is extracted
    * @return sub-schema for `relType`
    */
  def forRelationship(relType: CTRelationship): Schema = {
    val givenRelTypes = if (relType.types.isEmpty) {
      relationshipTypes
    } else {
      relType.types
    }

    copy(
      labels = Set.empty,
      relationshipTypes = givenRelTypes,
      nodeKeyMap = PropertyKeyMap.empty,
      relKeyMap = this.relKeyMap.filterByClassifier(givenRelTypes),
      impliedLabels = ImpliedLabels.empty,
      labelCombinations = LabelCombinations.empty
    )
  }

  /**
    * Computes the resulting implied labels from the current and the given schema.
    *
    * Example:
    *
    * this.labels = {A, B, C}
    * this.impliedLabels:
    * A -> B
    * B -> C
    *
    * other.labels = {B, C, D}
    * other.impliedLabels
    * B -> C
    * C -> D
    *
    * (1) compute intersection between this.impliedLabels and other.impliedLabels
    * (2) compute implied labels exclusive for left and right side
    * (3) from exclusive labels remove those pairs where the first item is not contained in the other one's label set
    * (4) union intersecting pairs with filtered pairs from both sides
    *
    * @param other other schema
    * @return implied labels inferred from the given implied labels
    */
  private def inferImpliedLabels(other: Schema) = {
    val leftImpliedPairs = this.impliedLabels.toPairs
    val rightImpliedPairs = other.impliedLabels.toPairs

    val intersectPairs = leftImpliedPairs intersect rightImpliedPairs
    val exclusivePairsLeft = (leftImpliedPairs -- intersectPairs)
      .filterNot(pair => other.labels.contains(pair._1))
    val exclusivePairsRight = (rightImpliedPairs -- intersectPairs)
      .filterNot(pair => this.labels.contains(pair._1))

    ImpliedLabels((exclusivePairsLeft ++ exclusivePairsRight ++ intersectPairs)
      .groupBy(_._1)
      .map(pair => pair._1 -> pair._2.map(_._2)))
  }

  override def verify: VerifiedSchema = {
    if (conflictSet.nonEmpty) {
      throw new IllegalStateException(s"Schema invalid: $self")
    }

    val coOccurringLabels =
      for (
        label <- labels;
        other <- impliedLabels(Set(label)) ++ labelCombination(Set(label))
      ) yield label -> other

    for ((label, other) <- coOccurringLabels) {
      val xKeys = nodeKeys(label)
      val yKeys = nodeKeys(other)
      if (xKeys.keySet.intersect(yKeys.keySet).exists(key => xKeys(key) != yKeys(key)))
        throw new IllegalArgumentException(s"Failed to verify schema for labels :$label and :$other")
    }

    new VerifiedSchema {
      override def schema: Schema = self
    }
  }

  override def toString: String =
    if (isEmpty) "empty schema"
    else {
      import scala.compat.Platform.EOL

      val builder = new StringBuilder

      if (labels.nonEmpty) {
        builder.append(s"Node labels {$EOL")
        labels.foreach { label =>
          builder.append(s"\t:$label$EOL")
          nodeKeys(label).foreach {
            case (key, typ) => builder.append(s"\t\t$key: $typ$EOL")
          }
        }
        builder.append(s"}$EOL")
      } else {
        builder.append(s"no labels$EOL")
      }

      if (impliedLabels.m.nonEmpty) {
        builder.append(s"Implied labels:$EOL")
        impliedLabels.m.foreach { pair =>
          builder.append(s":${pair._1} -> ${pair._2}$EOL")
        }
      } else {
        builder.append(s"no label implications$EOL")
      }

      if (labelCombinations.combos.nonEmpty) {
        builder.append(s"Label combinations:$EOL")
        labelCombinations.combos.foreach { set =>
          builder.append(s"$set$EOL")
        }
      } else {
        builder.append(s"no label combinations$EOL")
      }

      if (relationshipTypes.nonEmpty) {
        builder.append(s"Rel types {$EOL")
        relationshipTypes.foreach { relType =>
          builder.append(s"\t:$relType$EOL")
          relationshipKeys(relType).foreach {
            case (key, typ) => builder.append(s"\t\t$key: $typ$EOL")
          }
        }
        builder.append(s"}$EOL")
      } else {
        builder.append(s"no relationship types$EOL")
      }

      builder.toString
    }
}

sealed abstract class VerifiedSchema extends Verified[Schema] with Serializable {
  final override def v: Schema = schema
  def schema: Schema
}
