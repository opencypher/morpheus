/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.okapi.impl.schema

import cats.instances.all._
import cats.syntax.semigroup._
import org.opencypher.okapi.api.schema.LabelPropertyMap._
import org.opencypher.okapi.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.okapi.api.schema.RelTypePropertyMap._
import org.opencypher.okapi.api.schema.{LabelPropertyMap, RelTypePropertyMap, _}
import org.opencypher.okapi.api.types.CypherType.joinMonoid
import org.opencypher.okapi.api.types.{CypherType, _}
import org.opencypher.okapi.impl.exception.SchemaException
import org.opencypher.okapi.impl.schema.SchemaImpl._
import org.opencypher.okapi.impl.util.Version
import ujson.Js.{Num, Obj, Str}
import upickle.Js
import upickle.default.{macroRW, _}

object SchemaImpl {

  val VERSION = "version"
  val LABEL_PROPERTY_MAP = "labelPropertyMap"
  val REL_TYPE_PROPERTY_MAP = "relTypePropertyMap"
  val NODE_KEYS = "nodeKeys"
  val REL_KEYS = "relKeys"
  val SCHEMA_PATTERNS = "schemaPatterns"

  val LABELS = "labels"
  val REL_TYPE = "relType"
  val PROPERTIES = "properties"


  implicit def rw: ReadWriter[Schema] = readwriter[Js.Value].bimap[Schema](
    schema => {
      val tuples: Seq[(String, Js.Value)] = Seq[(String, Js.Value)](
        VERSION -> writeJs(Schema.CURRENT_VERSION.toString),
        LABEL_PROPERTY_MAP -> writeJs(schema.labelPropertyMap),
        REL_TYPE_PROPERTY_MAP -> writeJs(schema.relTypePropertyMap)) ++ {
        if (schema.explicitSchemaPatterns.nonEmpty) {
          Some(SCHEMA_PATTERNS -> writeJs(schema.explicitSchemaPatterns))
        } else {
          None
        }
      } ++ {
        if (schema.nodeKeys.nonEmpty) {
          Some(NODE_KEYS -> writeJs(schema.nodeKeys))
        } else {
          None
        }
      } ++ {
        if (schema.relationshipKeys.nonEmpty) {
          Some(REL_KEYS -> writeJs(schema.relationshipKeys))
        } else {
          None
        }
      }
      Js.Obj.from(tuples)
    }
    ,
    json => {
      val versionString: String = json.obj(VERSION) match {
        case Str(inner) => inner
        case Num(inner) => inner.toString
        case other => throw SchemaException(s"Expected Version to be a String or a Number but got $other")
      }
      val version = Version(versionString)
      if(!version.compatibleWith(Schema.CURRENT_VERSION)) throw SchemaException("Incompatible Schema versions")

      val labelPropertyMap = readJs[LabelPropertyMap](json.obj(LABEL_PROPERTY_MAP))
      val relTypePropertyMap = readJs[RelTypePropertyMap](json.obj(REL_TYPE_PROPERTY_MAP))
      val explicitSchemaPatterns = json match {
        case Obj(m) if m.keySet.contains(SCHEMA_PATTERNS) => readJs[Set[SchemaPattern]](json.obj(SCHEMA_PATTERNS))
        case _ => Set.empty[SchemaPattern]
      }
      val nodeKeys = json match {
        case Obj(m) if m.keySet.contains(NODE_KEYS) => readJs[Map[String, Set[String]]](json.obj(NODE_KEYS))
        case _ => Map.empty[String, Set[String]]
      }
      val relKeys = json match {
        case Obj(m) if m.keySet.contains(REL_KEYS) => readJs[Map[String, Set[String]]](json.obj(REL_KEYS))
        case _ => Map.empty[String, Set[String]]
      }
      SchemaImpl(labelPropertyMap, relTypePropertyMap, explicitSchemaPatterns, nodeKeys, relKeys)
    }
  )

  implicit def lpmRw: ReadWriter[LabelPropertyMap] = readwriter[Js.Value].bimap[LabelPropertyMap](
    labelPropertyMap =>
      labelPropertyMap.map {
        case (labelCombo, propKeys) => Js.Obj(LABELS -> writeJs(labelCombo), PROPERTIES -> writeJs(propKeys))
      },
    json =>
      json.arr.map { value =>
        readJs[Set[String]](value.obj(LABELS)) -> readJs[PropertyKeys](value.obj(PROPERTIES))
      }.toMap
  )

  implicit def rpmRw: ReadWriter[RelTypePropertyMap] = readwriter[Js.Value].bimap[RelTypePropertyMap](
    relTypePropertyMap =>
      relTypePropertyMap.map {
        case (relType, propKeys) => Js.Obj(REL_TYPE -> writeJs(relType), PROPERTIES -> writeJs(propKeys))
      },
    json =>
      json.arr.map { value =>
        readJs[String](value.obj(REL_TYPE)) -> readJs[PropertyKeys](value.obj(PROPERTIES))
      }.toMap
  )

  implicit def spRW: ReadWriter[SchemaPattern] = macroRW
}

final case class SchemaImpl(
  labelPropertyMap: LabelPropertyMap,
  relTypePropertyMap: RelTypePropertyMap,
  explicitSchemaPatterns: Set[SchemaPattern] = Set.empty,
  override val nodeKeys: Map[String, Set[String]] = Map.empty,
  override val relationshipKeys: Map[String, Set[String]] = Map.empty
) extends Schema {

  self: Schema =>

  lazy val labels: Set[String] = labelPropertyMap.keySet.flatten

  lazy val relationshipTypes: Set[String] = relTypePropertyMap.keySet

  override lazy val schemaPatterns: Set[SchemaPattern] = {
    if (explicitSchemaPatterns.nonEmpty) {
      explicitSchemaPatterns
    } else {
      for {
        source <- labelCombinations.combos
        relType <- relationshipTypes
        target <- labelCombinations.combos
      } yield SchemaPattern(source, relType, target)
    }
  }

  override lazy val impliedLabels: ImpliedLabels = {
    val implications = self.labelCombinations.combos.foldLeft(Map.empty[String, Set[String]]) {
      case (currentMap, combo) => combo.foldLeft(currentMap) {
        case (innerMap, label) => innerMap.get(label) match {
          case Some(innerCombo) => innerMap.updated(label, (innerCombo intersect combo) - label)
          case None => innerMap.updated(label, combo - label)
        }
      }
    }
    ImpliedLabels(implications)
  }

  override lazy val labelCombinations: LabelCombinations =
    LabelCombinations(labelPropertyMap.labelCombinations)

  override def impliedLabels(knownLabels: Set[String]): Set[String] =
    impliedLabels.transitiveImplicationsFor(knownLabels.intersect(labels))

  override def nodePropertyKeys(labelCombination: Set[String]): PropertyKeys = labelPropertyMap.properties(labelCombination)

  override def allCombinations: Set[Set[String]] =
    combinationsFor(Set.empty)

  override def combinationsFor(knownLabels: Set[String]): Set[Set[String]] =
    labelCombinations.combinationsFor(knownLabels)

  override def nodePropertyKeyType(knownLabels: Set[String], key: String): Option[CypherType] = {
    val combos = combinationsFor(knownLabels)
    nodePropertyKeysForCombinations(combos).get(key)
  }

  override def nodePropertyKeysForCombinations(labelCombinations: Set[Set[String]]): PropertyKeys = {
    val allKeys = labelCombinations.toSeq.flatMap(nodePropertyKeys)
    val propertyKeys = allKeys.groupBy(_._1).mapValues { seq =>
      if (seq.size == labelCombinations.size && seq.forall(seq.head == _)) {
        seq.head._2
      } else if (seq.size < labelCombinations.size) {
        seq.map(_._2).foldLeft(CTNull: CypherType)(_ join _)
      } else {
        seq.map(_._2).reduce(_ join _)
      }
    }

    propertyKeys.view.force
  }

  override def relationshipPropertyKeyType(types: Set[String], key: String): Option[CypherType] = {
    // relationship types have OR semantics: empty set means all types
    val relevantTypes = if (types.isEmpty) relationshipTypes else types

    relevantTypes.map(relationshipPropertyKeys).foldLeft(CTVoid: CypherType) {
      case (inferred, next) => inferred.join(next.getOrElse(key, CTNull))
    } match {
      case CTNull => None
      case tpe => Some(tpe)
    }
  }

  override def relationshipPropertyKeysForTypes(knownTypes: Set[String]): PropertyKeys = {
    val relevantTypes = if (knownTypes.isEmpty) relationshipTypes else knownTypes

    val allKeys = relevantTypes.flatMap(relationshipPropertyKeys)
    val propertyKeys = allKeys.groupBy(_._1).mapValues { seq =>
      if (seq.size == relevantTypes.size && seq.forall(seq.head == _)) {
        seq.head._2
      } else if (seq.size < relevantTypes.size) {
        seq.map(_._2).foldLeft(CTNull: CypherType)(_ join _)
      } else {
        seq.map(_._2).reduce(_ join _)
      }
    }

    propertyKeys.view.force
  }

  override def relationshipPropertyKeys(typ: String): PropertyKeys = relTypePropertyMap.properties(typ)

  override def schemaPatternsFor(
    knownSourceLabels: Set[String],
    knownRelTypes: Set[String],
    knownTargetLabels: Set[String]
  ): Set[SchemaPattern] = {
    val possibleSourcePatterns = schemaPatterns.filter(p => knownSourceLabels.subsetOf(p.sourceLabelCombination))
    val possibleRelTypePatterns = possibleSourcePatterns.filter(p => knownRelTypes.contains(p.relType) || knownRelTypes.isEmpty)
    val possibleTargetPatterns = possibleRelTypePatterns.filter(p => knownTargetLabels.subsetOf(p.targetLabelCombination))

    possibleTargetPatterns
  }

  override def withNodePropertyKeys(labelCombination: Set[String], keys: PropertyKeys): Schema = {
    if (labelCombination.exists(_.isEmpty))
      throw SchemaException("Labels must be non-empty")
    val propertyKeys = if (labelPropertyMap.labelCombinations(labelCombination)) {
      computePropertyTypes(labelPropertyMap.properties(labelCombination), keys)
    } else {
      keys
    }
    copy(labelPropertyMap = labelPropertyMap.register(labelCombination, propertyKeys))
  }

  override def withNodeKey(label: String, nodeKey: Set[String]): Schema = {
    if (!labels.contains(label)) {
      throw SchemaException(
        s"""|Invalid node key.
            |Unknown node label `$label`.
            |Should be one of: ${labels.mkString("[", ", ", "]")}""".stripMargin)
    }

    val propertyKeys = nodePropertyKeysForCombinations(combinationsFor(Set(label)))

    if (!nodeKey.subsetOf(propertyKeys.keySet)) {
      throw SchemaException(
        s"""|Invalid node key.
            |Not all combinations that contain `$label` have all the properties for ${nodeKey.mkString("[", ", ", "]")}.
            |Available keys: ${propertyKeys.keySet.mkString("[", ", ", "]")}""".stripMargin)
    }

    val nullableProperties = propertyKeys.filter {
      case (key, tpe) => nodeKey.contains(key) && tpe.isNullable
    }

    if (nullableProperties.nonEmpty) {
      throw SchemaException(
        s"""|Invalid node key.
            |Properties ${nullableProperties.keySet.mkString("[", ", ", "]")} have nullable types.
            |Nullable properties can not be part of a node key.""".stripMargin)
    }

    copy(nodeKeys = nodeKeys.updated(label, nodeKey))

  }

  override def withRelationshipKey(relationshipType: String, relationshipKey: Set[String]): Schema = {
    if (!relationshipTypes.contains(relationshipType)) {
      throw SchemaException(
        s"""|Invalid relationship key.
            |Unknown relationship type `$relationshipType`.
            |Should be one of: ${relationshipTypes.mkString("[", ", ", "]")}.""".stripMargin)
    }

    val propertyKeys = relationshipPropertyKeys(relationshipType)

    if (!relationshipKey.subsetOf(propertyKeys.keySet)) {
      throw SchemaException(
        s"""|Invalid relationship key.
            |Relationship type `$relationshipType` does not have all the properties for ${relationshipKey.mkString("[", ", ", "]")}.
            |Available keys: ${propertyKeys.keySet.mkString("[", ", ", "]")}""".stripMargin)
    }

    val nullableProperties = propertyKeys.filter {
      case (key, tpe) => relationshipKey.contains(key) && tpe.isNullable
    }

    if (nullableProperties.nonEmpty) {
      throw SchemaException(
        s"""|Invalid relationship key.
            |Properties ${nullableProperties.keySet.mkString("[", ", ", "]")} have nullable types.
            |Nullable properties can not be part of a relationship key.""".stripMargin)
    }

    copy(relationshipKeys = relationshipKeys.updated(relationshipType, relationshipKey))
  }

  private def computePropertyTypes(existing: PropertyKeys, input: PropertyKeys): PropertyKeys = {
    // Map over input keys to calculate join of type with existing type
    val keysWithJoinedTypes = input.map {
      case (key, propType) =>
        val inType = existing.getOrElse(key, CTNull)
        key -> propType.join(inType)
    }

    // Map over the rest of the existing keys to mark them all nullable
    val propertiesMarkedOptional = existing.filterKeys(k => !input.contains(k)).foldLeft(keysWithJoinedTypes) {
      case (map, (key, propTyp)) =>
        map.updated(key, propTyp.nullable)
    }

    propertiesMarkedOptional
  }

  override def withRelationshipPropertyKeys(typ: String, keys: PropertyKeys): Schema = {
    if (relationshipTypes contains typ) {
      val updatedTypes = computePropertyTypes(relTypePropertyMap.properties(typ), keys)

      copy(relTypePropertyMap = relTypePropertyMap.register(typ, updatedTypes.toSeq))
    } else {
      copy(relTypePropertyMap = relTypePropertyMap.register(typ, keys))
    }
  }

  override def withSchemaPatterns(patterns: SchemaPattern*): Schema = {
    patterns.foreach { p =>
      if (!labelCombinations.combos.contains(p.sourceLabelCombination)) throw SchemaException(s"Unknown source node label combination: `${p.sourceLabelCombination}`. Should be one of: ${labelCombinations.combos.mkString("[", ",", "]")}")
      if (!relationshipTypes.contains(p.relType)) throw SchemaException(s"Unknown relationship type: `${p.relType}`. Should be one of ${relationshipTypes.mkString("[", ",", "]")}")
      if (!labelCombinations.combos.contains(p.targetLabelCombination)) throw SchemaException(s"Unknown target node label combination: `${p.targetLabelCombination}`. Should be one of: ${labelCombinations.combos.mkString("[", ",", "]")}")
    }

    copy(explicitSchemaPatterns = explicitSchemaPatterns ++ patterns.toSet)
  }

  override def ++(other: Schema): SchemaImpl = {
    val conflictingLabels = labelPropertyMap.labelCombinations intersect other.labelPropertyMap.labelCombinations
    val nulledOut = conflictingLabels.foldLeft(Map.empty[Set[String], PropertyKeys]) {
      case (acc, next) =>
        val keys = computePropertyTypes(labelPropertyMap.properties(next), other.labelPropertyMap.properties(next))
        acc + (next -> keys)
    }
    val newLabelPropertyMap = labelPropertyMap |+| other.labelPropertyMap |+| nulledOut

    val conflictingRelTypes = relationshipTypes intersect other.relationshipTypes
    val nulledRelProps = conflictingRelTypes.foldLeft(Map.empty[String, PropertyKeys]) {
      case (acc, next) =>
        val keys = computePropertyTypes(relTypePropertyMap.properties(next), other.relTypePropertyMap.properties(next))
        acc + (next -> keys)
    }
    val newRelTypePropertyMap = relTypePropertyMap |+| other.relTypePropertyMap |+| nulledRelProps

    val newExplicitSchemaPatterns = explicitSchemaPatterns ++ other.explicitSchemaPatterns

    val newNodeKeys = nodeKeys |+| other.nodeKeys
    val newRelationshipKeys = relationshipKeys |+| other.relationshipKeys

    copy(
      labelPropertyMap = newLabelPropertyMap,
      relTypePropertyMap = newRelTypePropertyMap,
      explicitSchemaPatterns = newExplicitSchemaPatterns,
      nodeKeys = newNodeKeys,
      relationshipKeys = newRelationshipKeys
    )
  }

  def forNode(labelConstraints: Set[String]): Schema = {
    val requiredLabels = {
      val explicitLabels = labelConstraints
      val impliedLabels = this.impliedLabels.transitiveImplicationsFor(explicitLabels)
      explicitLabels union impliedLabels
    }

    val possibleLabels = if (labelConstraints.isEmpty) {
      allCombinations
    } else {
      // add required labels because they might not be present in the schema already (newly created)
      combinationsFor(requiredLabels) + requiredLabels
    }

    // take all label properties that might appear on the possible labels
    val newLabelPropertyMap: LabelPropertyMap = this.labelPropertyMap.filterKeys(possibleLabels.contains)

    // add labels that were specified in the constraints but are not present in source schema
    val updatedLabelPropertyMap = possibleLabels.foldLeft(newLabelPropertyMap) {
      case (agg, combo) => agg.register(combo, agg.properties(combo))
    }

    SchemaImpl(
      labelPropertyMap = updatedLabelPropertyMap,
      relTypePropertyMap = RelTypePropertyMap.empty
    )
  }

  def forRelationship(relType: CTRelationship): Schema = {
    val givenRelTypes = if (relType.types.isEmpty) {
      relationshipTypes
    } else {
      relType.types
    }

    val updatedRelTypePropertyMap = this.relTypePropertyMap.filterForRelTypes(givenRelTypes)
    val updatedMap = givenRelTypes.foldLeft(updatedRelTypePropertyMap) {
      case (map, givenRelType) =>
        if (!map.contains(givenRelType)) map.updated(givenRelType, PropertyKeys.empty) else map
    }

    SchemaImpl(
      labelPropertyMap = LabelPropertyMap.empty,
      relTypePropertyMap = updatedMap
    )
  }

  override def pretty: String =
    if (isEmpty) "empty schema"
    else {
      import scala.compat.Platform.EOL

      val builder = new StringBuilder

      if (labelPropertyMap.labelCombinations.nonEmpty) {
        builder.append(s"Node labels {$EOL")
        labelPropertyMap.labelCombinations.foreach { combo =>
          val labelStr = if (combo eq Set.empty) "(no label)" else combo.mkString(":", ":", "")
          builder.append(s"\t$labelStr$EOL")
          nodePropertyKeys(combo).foreach {
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
          relationshipPropertyKeys(relType).foreach {
            case (key, typ) => builder.append(s"\t\t$key: $typ$EOL")
          }
        }
        builder.append(s"}$EOL")
      } else {
        builder.append(s"no relationship types$EOL")
      }

      if (explicitSchemaPatterns.nonEmpty) {
        builder.append(s"Explicit schema patterns {$EOL")
        explicitSchemaPatterns.foreach(p =>
          builder.append(s"\t$p$EOL")
        )
        builder.append(s"}$EOL")
      }

      builder.toString
    }

  override def isEmpty: Boolean = this == Schema.empty

  override private[opencypher] def dropPropertiesFor(combo: Set[String]) =
    copy(labelPropertyMap = labelPropertyMap - combo)

  override private[opencypher] def withOverwrittenNodePropertyKeys(
    nodeLabels: Set[String],
    propertyKeys: PropertyKeys
  ) =
    copy(labelPropertyMap = labelPropertyMap.register(nodeLabels, propertyKeys))

  override private[opencypher] def withOverwrittenRelationshipPropertyKeys(
    relType: String,
    propertyKeys: PropertyKeys
  ) =
    copy(relTypePropertyMap = relTypePropertyMap.register(relType, propertyKeys))

  override def toJson: String = upickle.default.write[Schema](this, indent = 4)
}
