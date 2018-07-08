/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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

import org.opencypher.okapi.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.okapi.api.schema.{LabelPropertyMap, RelTypePropertyMap, Schema}
import org.opencypher.okapi.api.types.CypherType._
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.exception.SchemaException
import org.opencypher.okapi.impl.schema.SchemaImpl.rw
import org.opencypher.okapi.impl.schema.SchemaUtils._
import upickle.Js
import upickle.default._

object SchemaImpl {

  val VERSION = "version"
  val LABEL_PROPERTY_MAP = "labelPropertyMap"
  val REL_TYPE_PROPERTY_MAP = "relTypePropertyMap"

  val LABELS = "labels"
  val REL_TYPE = "relType"
  val PROPERTIES = "properties"


  implicit def rw: ReadWriter[Schema] = readwriter[Js.Value].bimap[Schema](
    schema => Js.Obj(
      VERSION -> Js.Num(1),
      LABEL_PROPERTY_MAP -> writeJs(schema.labelPropertyMap),
      REL_TYPE_PROPERTY_MAP -> writeJs(schema.relTypePropertyMap)),
    json => {
      val labelPropertyMap = readJs[LabelPropertyMap](json.obj(LABEL_PROPERTY_MAP))
      val relTypePropertyMap = readJs[RelTypePropertyMap](json.obj(REL_TYPE_PROPERTY_MAP))
      SchemaImpl(labelPropertyMap, relTypePropertyMap)
    }
  )

  implicit def lpmRw: ReadWriter[LabelPropertyMap] = readwriter[Js.Value].bimap[LabelPropertyMap](
    labelPropertyMap => {
      val jsonEntries = labelPropertyMap.map.map {
        case (labelCombo, propKeys) => Js.Obj(LABELS -> writeJs(labelCombo), PROPERTIES -> writeJs(propKeys))
      }
      jsonEntries
    },
    json => {
      val content = json.arr
        .map(value => readJs[Set[String]](value.obj(LABELS)) -> readJs[PropertyKeys](value.obj(PROPERTIES)))
      LabelPropertyMap(content.toMap)
    }
  )

  implicit def rpmRw: ReadWriter[RelTypePropertyMap] = readwriter[Js.Value].bimap[RelTypePropertyMap](
    relTypePropertyMap => {
      val jsonEntries = relTypePropertyMap.map.map {
        case (relType, propKeys) => Js.Obj(REL_TYPE -> writeJs(relType), PROPERTIES -> writeJs(propKeys))
      }
      jsonEntries
    },
    json => {
      val content = json.arr
        .map(value => readJs[String](value.obj(REL_TYPE)) -> readJs[PropertyKeys](value.obj(PROPERTIES)))
      RelTypePropertyMap(content.toMap)
    }
  )
}

final case class SchemaImpl(
  labelPropertyMap: LabelPropertyMap,
  relTypePropertyMap: RelTypePropertyMap) extends Schema {

  self: Schema =>

  lazy val labels: Set[String] = labelPropertyMap.map.keySet.flatten

  lazy val relationshipTypes: Set[String] = relTypePropertyMap.map.keySet

  private def graphContainsNodeWithoutLabel: Boolean = labelPropertyMap.map.keySet.contains(Set.empty)

  override lazy val impliedLabels: ImpliedLabels = {
    val implications = self.foldAndProduce(Map.empty[String, Set[String]])(_ intersect _ - _, _ - _)

    ImpliedLabels(implications)
  }

  override lazy val labelCombinations: LabelCombinations =
    LabelCombinations(labelPropertyMap.labelCombinations)

  override def impliedLabels(knownLabels: Set[String]): Set[String] =
    impliedLabels.transitiveImplicationsFor(knownLabels.intersect(labels))

  override def nodeKeys(labels: Set[String]): PropertyKeys = labelPropertyMap.properties(labels)

  override def allNodeKeys: PropertyKeys = {
    val keyToTypes = allLabelCombinations
      .map(nodeKeys)
      .toSeq
      .flatten
      .groupBy(_._1)
      .map {
        case (k, v) => k -> v.map(_._2)
      }

    keyToTypes
      .mapValues(types => types.foldLeft[CypherType](CTVoid)(_ union _))
      .map {
        case (key, tpe) =>
          if (allLabelCombinations.map(nodeKeys).forall(_.get(key).isDefined))
            key -> tpe
          else key -> tpe.nullable
      }
  }

  override def allLabelCombinations: Set[Set[String]] =
    combinationsFor(Set.empty)

  override def combinationsFor(knownLabels: Set[String]): Set[Set[String]] =
    labelCombinations.combinationsFor(knownLabels)

  override def nodeKeyType(labels: Set[String], key: String): Option[CypherType] = {
    val combos = combinationsFor(labels)
    keysFor(combos).get(key)
  }

  override def keysFor(labelCombinations: Set[Set[String]]): PropertyKeys = {
    val allKeys = labelCombinations.toSeq.flatMap(nodeKeys)
    val propertyKeys = allKeys.groupBy(_._1).mapValues { seq =>
      if (seq.size == labelCombinations.size && seq.forall(seq.head == _)) {
        seq.head._2
      } else if (seq.size < labelCombinations.size) {
        seq.map(_._2).foldLeft(CTNull: CypherType)(_ union _)
      } else {
        seq.map(_._2).reduce(_ union _)
      }
    }

    propertyKeys
  }

  override def relationshipKeyType(types: Set[String], key: String): Option[CypherType] = {
    // relationship types have OR semantics: empty set means all types
    val relevantTypes = if (types.isEmpty) relationshipTypes else types

    relevantTypes.map(relationshipKeys).foldLeft(CTVoid: CypherType) {
      case (inferred, next) => inferred.union(next.getOrElse(key, CTNull))
    } match {
      case CTNull => None
      case tpe => Some(tpe)
    }
  }

  override def relationshipKeys(typ: String): PropertyKeys = relTypePropertyMap.properties(typ)

  override def withNodePropertyKeys(nodeLabels: Set[String], keys: PropertyKeys): Schema = {
    if (nodeLabels.exists(_.isEmpty))
      throw SchemaException("Labels must be non-empty")
    val propertyKeys = if (labelPropertyMap.labelCombinations(nodeLabels)) {
      computePropertyTypes(labelPropertyMap.properties(nodeLabels), keys)
    } else {
      keys
    }
    copy(labelPropertyMap = labelPropertyMap.register(nodeLabels, propertyKeys))
  }

  private def computePropertyTypes(existing: PropertyKeys, input: PropertyKeys): PropertyKeys = {
    // Map over input keys to calculate join of type with existing type
    val keysWithJoinedTypes = input.map {
      case (key, propType) =>
        val inType = existing.getOrElse(key, CTNull)
        key -> propType.union(inType)
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

  override def ++(other: Schema): SchemaImpl = {
    val conflictingLabels = labelPropertyMap.labelCombinations intersect other.labelPropertyMap.labelCombinations
    val nulledOut = conflictingLabels.foldLeft(Map.empty[Set[String], PropertyKeys]) {
      case (acc, next) =>
        val keys = computePropertyTypes(labelPropertyMap.properties(next), other.labelPropertyMap.properties(next))
        acc + (next -> keys)
    }
    val newNodeKeyMap = labelPropertyMap ++ other.labelPropertyMap ++ LabelPropertyMap(nulledOut)


    val conflictingRelTypes = relationshipTypes intersect other.relationshipTypes
    val nulledRelProps = conflictingRelTypes.foldLeft(Map.empty[String, PropertyKeys]) {
      case (acc, next) =>
        val keys = computePropertyTypes(relTypePropertyMap.properties(next), other.relTypePropertyMap.properties(next))
        acc + (next -> keys)
    }
    val newRelTypePropertyMap = relTypePropertyMap ++ other.relTypePropertyMap ++ RelTypePropertyMap(nulledRelProps)

    copy(labelPropertyMap = newNodeKeyMap, relTypePropertyMap = newRelTypePropertyMap)
  }

  def forNode(labelConstraints: Set[String]): Schema = {
    val requiredLabels = {
      val explicitLabels = labelConstraints
      val impliedLabels = this.impliedLabels.transitiveImplicationsFor(explicitLabels)
      explicitLabels union impliedLabels
    }

    val possibleLabels = if (labelConstraints.isEmpty) {
      allLabelCombinations
    } else {
      // add required labels because they might not be present in the schema already (newly created)
      combinationsFor(requiredLabels) + requiredLabels
    }

    // take all label properties that might appear on the possible labels
    val newLabelPropertyMap = LabelPropertyMap(this.labelPropertyMap.map.filterKeys(possibleLabels.contains))

    // add labels that were specified in the constraints but are not present in source schema
    val updatedLabelPropertyMap = possibleLabels.foldLeft(newLabelPropertyMap) {
      case (agg, combo) => agg.register(combo, agg.properties(combo))
    }

    SchemaImpl(
      labelPropertyMap = updatedLabelPropertyMap,
      relTypePropertyMap = RelTypePropertyMap.empty
    )
  }

  def forRelationship(relType: CypherType): Schema = {
    ???
    // TODO: Add helper to CypherType
//    val givenRelTypes = if (AnyRelationship.subTypeOf(relType)) {
//      relationshipTypes
//    } else {
//      relType.alternatives.collect { case r: CTRelationship => r }.flatMap(_.relType)
//    }
//
//    val updatedRelTypePropertyMap = this.relTypePropertyMap.filterForRelTypes(givenRelTypes)
//    val updatedMap = givenRelTypes.foldLeft(updatedRelTypePropertyMap.map) {
//      case (map, givenRelType) =>
//        if (!map.contains(givenRelType)) map.updated(givenRelType, PropertyKeys.empty) else map
//    }
//
//    SchemaImpl(
//      labelPropertyMap = LabelPropertyMap.empty,
//      relTypePropertyMap = RelTypePropertyMap(updatedMap)
//    )
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

  override def isEmpty: Boolean = this == Schema.empty

  override private[opencypher] def dropPropertiesFor(combo: Set[String]) =
    copy(labelPropertyMap - combo)

  override private[opencypher] def withOverwrittenNodePropertyKeys(nodeLabels: Set[String], propertyKeys: PropertyKeys) =
    copy(labelPropertyMap = labelPropertyMap.register(nodeLabels, propertyKeys))

  override private[opencypher] def withOverwrittenRelationshipPropertyKeys(relType: String, propertyKeys: PropertyKeys) =
    copy(relTypePropertyMap = relTypePropertyMap.register(relType, propertyKeys))

  override def toJson: String = upickle.default.write[Schema](this, indent = 4)
}
