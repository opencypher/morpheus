/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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

import org.opencypher.caps.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.caps.api.types._
import org.opencypher.caps.impl.exception.SchemaException

import scala.language.{existentials, implicitConversions} // fix compiler warning

object Schema {
  val empty: Schema = Schema(
    labels = Set.empty,
    relationshipTypes = Set.empty,
    labelPropertyMap = LabelPropertyMap.empty,
    relTypePropertyMap = RelTypePropertyMap.empty
  )
}

final case class Schema(
  // TODO: Move labels and relationship types away from constructor
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
  labelPropertyMap: LabelPropertyMap,

  /**
    * Property keys associated with a relationship type
    */
  relTypePropertyMap: RelTypePropertyMap) {

  self: Schema =>

  private def graphContainsNodeWithoutLabel: Boolean = labelPropertyMap.map.keySet.contains(Set.empty)

  /**
    * Implied labels for each existing label
    */
  lazy val impliedLabels: ImpliedLabels = {
    val implications = foldAndProduce(Map.empty[String, Set[String]])(_ intersect _ - _, _ - _)

    ImpliedLabels(implications)
  }
  /**
    * Groups of labels where each group contains possible label combinations.
    */
  lazy val labelCombinations: LabelCombinations =
    LabelCombinations(labelPropertyMap.labelCombinations)

  /**
    * Given a set of labels that a node definitely has, returns all labels the node _must_ have.
    */
  def impliedLabels(knownLabels: String*): Set[String] =
    impliedLabels(knownLabels.toSet)

  /**
    * Given a set of labels that a node definitely has, returns all labels the node _must_ have.
    */
  def impliedLabels(knownLabels: Set[String]): Set[String] =
    impliedLabels.transitiveImplicationsFor(knownLabels.intersect(labels))

  def nodeKeys(labels: String*): PropertyKeys = nodeKeys(labels.toSet)

  /**
    * Given a set of labels that a node definitely has, returns its property schema.
    *
    * TODO: consider implied labels here?
    */
  def nodeKeys(labels: Set[String]): PropertyKeys = labelPropertyMap.properties(labels)

  def nodeKeys(label: String): PropertyKeys = nodeKeys(Set(label))

  def allNodeKeys: PropertyKeys = {
    val keyToTypes = allLabelCombinations
      .map(nodeKeys)
      .toSeq
      .flatten
      .groupBy(_._1)
      .map {
        case (k, v) => k -> v.map(_._2)
      }

    keyToTypes
      .mapValues(types => types.foldLeft[CypherType](CTVoid)(_ join _))
      .map {
        case (key, tpe) =>
          if (allLabelCombinations.map(nodeKeys).forall(_.get(key).isDefined))
            key -> tpe
          else key -> tpe.nullable
      }
  }

  def allLabelCombinations: Set[Set[String]] =
    combinationsFor(Set.empty)

  /**
    * Given a set of labels that a node definitely has, returns all combinations of labels that the node could possibly have.
    */
  def combinationsFor(knownLabels: Set[String]): Set[Set[String]] =
    labelCombinations.combinationsFor(knownLabels)

  /**
    * Computes the type (if any) for a property given a predicate of labels.
    *
    * @param labels the labels predicate; either a lower bound of expected labels, or a disjunction of possible labels
    * @param key    the property key
    * @return the Cypher type, if any, mapped to the key for nodes that pass the predicate
    */
  def nodeKeyType(labels: Set[String], key: String): Option[CypherType] = {
    val combos = combinationsFor(labels)
    keysFor(combos).get(key)
  }

  /**
    * Computes property keys for the set of label combinations.
    *
    * @param labelCombinations label combinations to consider.
    * @return typed property keys, with joined or nullable types for conflicts.
    */
  def keysFor(labelCombinations: Set[Set[String]]): PropertyKeys = {
    val allKeys = labelCombinations.toSeq.flatMap(nodeKeys)
    val propertyKeys = allKeys.groupBy(_._1).mapValues { seq =>
      if (seq.size == labelCombinations.size && seq.forall(seq.head == _)) {
        seq.head._2
      } else if (seq.size < labelCombinations.size) {
        seq.map(_._2).foldLeft(CTNull: CypherType)(_ join _)
      } else {
        seq.map(_._2).reduce(_ join _)
      }
    }

    propertyKeys
  }

  /**
    * Returns the property keys that exist on any of the labels.
    * Types of conflicting property keys are joined.
    *
    * @param labels labels for which to return the property keys
    * @return property keys for labels
    */
  def keysFor(labels: String*): PropertyKeys = {
    keysFor(labels.map(Set(_)).toSet)
  }

  def relationshipKeyType(types: Set[String], key: String): Option[CypherType] = {
    // relationship types have OR semantics: empty set means all types
    val relevantTypes = if (types.isEmpty) relationshipTypes else types

    relevantTypes.map(relationshipKeys).foldLeft(CTVoid: CypherType) {
      case (inferred, next) => inferred.join(next.getOrElse(key, CTNull))
    } match {
      case CTNull => None
      case tpe => Some(tpe)
    }
  }

  /**
    * Returns the property schema for a given relationship type
    */
  def relationshipKeys(typ: String): Map[String, CypherType] = relTypePropertyMap.properties(typ)

  def withNodePropertyKeys(nodeLabels: String*)(keys: (String, CypherType)*): Schema =
    withNodePropertyKeys(nodeLabels.toSet, keys.toMap)

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
    * @param nodeLabels the node labels to add to the schema
    * @param keys       the typed property keys to associate with the labels
    * @return a copy of the Schema with the provided new data
    */
  def withNodePropertyKeys(nodeLabels: Set[String], keys: PropertyKeys): Schema = {
    if (nodeLabels.exists(_.isEmpty))
      throw SchemaException("Labels must be non-empty")
    val propertyKeys = if (labelPropertyMap.labelCombinations(nodeLabels)) {
      computePropertyTypes(labelPropertyMap.properties(nodeLabels), keys)
    } else {
      keys
    }
    copy(labels = labels union nodeLabels, labelPropertyMap = labelPropertyMap.register(nodeLabels, propertyKeys))
  }

  private def computePropertyTypes(existing: PropertyKeys, input: PropertyKeys): PropertyKeys = {
    // Map over input keys to calculate join of type with existing type
    val keysWithJoinedTypes = input.map {
      case (key, propType) =>
        val inType = existing.getOrElse(key, CTNull)
        key -> sparkCompatibleJoin(None, key, propType, inType)
    }

    // Map over the rest of the existing keys to mark them all nullable
    val propertiesMarkedOptional = existing.filterKeys(k => !input.contains(k)).foldLeft(keysWithJoinedTypes) {
      case (map, (key, propTyp)) =>
        map.updated(key, propTyp.nullable)
    }

    propertiesMarkedOptional
  }

  def sparkCompatibleJoin(label: Option[String], key: String, t1: CypherType, t2: CypherType): CypherType = {
    val join = t1.join(t2)
    if (join.material == t1.material || join.material == t2.material) {
      join
    } else
      throw new SchemaException(
        s"The property types $t1 and $t2 (for property '$key'${label.map(l => s" and label '$l'").getOrElse("")}) " +
          s"can not be stored in the same Spark column")
  }

  def withRelationshipType(relType: String): Schema =
    withRelationshipPropertyKeys(relType)()

  def withRelationshipPropertyKeys(typ: String)(keys: (String, CypherType)*): Schema =
    withRelationshipPropertyKeys(typ, keys.toMap)

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
    * @param typ  the relationship type to add to the schema
    * @param keys the properties (name and type) to associate with the relationship type
    * @return a copy of the Schema with the provided new data
    */
  def withRelationshipPropertyKeys(typ: String, keys: PropertyKeys): Schema = {
    if (relationshipTypes contains typ) {
      val updatedTypes = computePropertyTypes(relTypePropertyMap.properties(typ), keys)

      copy(relTypePropertyMap = relTypePropertyMap.register(typ, updatedTypes.toSeq))
    } else {
      copy(relationshipTypes = relationshipTypes + typ, relTypePropertyMap = relTypePropertyMap.register(typ, keys))
    }
  }

  def ++(other: Schema): Schema = {
    val newLabels = labels ++ other.labels
    val newRelTypes = relationshipTypes ++ other.relationshipTypes
    val newRelTypePropertyMap = relTypePropertyMap ++ other.relTypePropertyMap
    val conflictingLabels = labelPropertyMap.labelCombinations intersect other.labelPropertyMap.labelCombinations
    val nulledOut = conflictingLabels.foldLeft(Map.empty[Set[String], PropertyKeys]) {
      case (acc, next) =>
        val keys = computePropertyTypes(labelPropertyMap.properties(next), other.labelPropertyMap.properties(next))
        acc + (next -> keys)
    }
    val newNodeKeyMap = labelPropertyMap ++ other.labelPropertyMap ++ LabelPropertyMap(nulledOut)

    copy(newLabels, newRelTypes, newNodeKeyMap, newRelTypePropertyMap).verify
  }

  // Verification makes sure that we will always know the exact type of a property when given at least one label
  // Another, more restrictive verification would be to guarantee that even without a label
  def verify: Schema = {
    val combosByLabel = foldAndProduce(Map.empty[String, Set[Set[String]]])({ (set, combos, _) =>
      set + combos
    }, (combos, _) => Set(combos))

    combosByLabel.foreach {
      case (label, combos) =>
        val keysForAllCombosOfLabel = combos.map(nodeKeys)
        for {
          keys1 <- keysForAllCombosOfLabel
          keys2 <- keysForAllCombosOfLabel
          if keys1 != keys2
        } yield {
          (keys1.keySet intersect keys2.keySet).foreach { k =>
            val t1 = keys1(k)
            val t2 = keys2(k)
            sparkCompatibleJoin(Some(label), k, t1, t2)
          }
        }
    }
    self
  }

  private def foldAndProduce[A](
    zero: Map[String, A])(bound: (A, Set[String], String) => A, fresh: (Set[String], String) => A) = {
    labelPropertyMap.labelCombinations.foldLeft(zero) {
      case (map, labelCombos) =>
        labelCombos.foldLeft(map) {
          case (innerMap, label) =>
            innerMap.get(label) match {
              case Some(a) =>
                innerMap.updated(label, bound(a, labelCombos, label))
              case None =>
                innerMap.updated(label, fresh(labelCombos, label))
            }
        }
    }
  }

  /**
    * Given the current schema, construct a new Schema for an entity with a given set of labels.
    * If the set of labels is empty, this means that the resulting schema will only have properties present on nodes
    * that have no labels.
    *
    * @param labels labels present on the node entity
    * @return
    */
  def fromNodeEntity(labels: Set[String]): Schema = {
    if(labels.nonEmpty) {
      forNodeScan(labels)
    } else {
      val propertyKeys = if(graphContainsNodeWithoutLabel) {
        labelPropertyMap.properties(Set.empty[String])
      } else {
        PropertyKeys.empty
      }
      Schema.empty.withNodePropertyKeys(Set.empty[String], propertyKeys)
    }
  }

  /**
    * Returns the sub-schema for a node scan under the given constraints.
    * Labels are interpreted as constraints on the resulting Schema.
    * If no labels are specified, this means the resulting node can have any valid label combination.
    *
    * @param labelConstraints Specifies the labels that the node is guaranteed to have
    * @return sub-schema for `labelConstraints`
    */
  def forNodeScan(labelConstraints: Set[String]): Schema = {
    val requiredLabels = {
      val explicitLabels = labelConstraints
      val impliedLabels = this.impliedLabels.transitiveImplicationsFor(explicitLabels)
      explicitLabels union impliedLabels
    }

    val possibleLabels = if (labelConstraints.isEmpty) {
      allLabelCombinations
    } else {
      // add required labels because they might not be present in the schema already (newly created)
      this.labelCombinations.filterByLabels(requiredLabels).combos + requiredLabels
    }

    val newLabelPropertyMap = LabelPropertyMap(this.labelPropertyMap.map.filterKeys(possibleLabels.contains))

    // add labels that were specified in the constraints but are not present in source schema
    val updatedLabelPropertyMap = possibleLabels.flatten.foldLeft(newLabelPropertyMap) {
      case (agg, label) if agg.map.keys.exists(_.contains(label)) => agg
      case (agg, label) => agg.register(label)()
    }

    copy(
      labels = possibleLabels.flatten,
      Set.empty,
      labelPropertyMap = updatedLabelPropertyMap,
      relTypePropertyMap = RelTypePropertyMap.empty
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

    val updatedRelTypePropertyMap = this.relTypePropertyMap.filterForRelTypes(givenRelTypes)
    val updatedMap = givenRelTypes.foldLeft(updatedRelTypePropertyMap.map) {
      case (map, givenRelType) =>
        if (!map.contains(givenRelType)) map.updated(givenRelType, PropertyKeys.empty) else map
    }

    copy(
      labels = Set.empty,
      relationshipTypes = givenRelTypes,
      labelPropertyMap = LabelPropertyMap.empty,
      relTypePropertyMap = RelTypePropertyMap(updatedMap)
    )
  }


  override def toString: String = "Schema"

  def pretty: String =
    if (isEmpty) "empty schema"
    else {
      import scala.compat.Platform.EOL

      val builder = new StringBuilder

      if (labelPropertyMap.labelCombinations.nonEmpty) {
        builder.append(s"Node labels {$EOL")
        labelPropertyMap.labelCombinations.foreach { combo =>
          val labelStr = if (combo eq Set.empty) "(no label)" else combo.mkString(":", ":", "")
          builder.append(s"\t$labelStr$EOL")
          nodeKeys(combo).foreach {
            case (key, typ) => builder.append(s"\t\t$key: $typ$EOL")
          }
        }
        builder.append(s"}$EOL")
      } else {
        builder.append(s"no labels$EOL")
      }

      if (impliedLabels.m.exists(_._2.nonEmpty)) {
        builder.append(s"Implied labels:$EOL")
        impliedLabels.m.foreach {
          case (label, implications) if implications.nonEmpty =>
            builder.append(s":$label -> ${implications.mkString(":", ":", "")}$EOL")
          case _ =>
        }
      } else {
        builder.append(s"no label implications$EOL")
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

  def isEmpty: Boolean = this == Schema.empty
}
