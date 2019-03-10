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
package org.opencypher.spark.api.io.sql

import org.opencypher.graphddl.{ElementType, GraphType}
import org.opencypher.okapi.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.okapi.api.schema.{Schema, SchemaPattern}
import org.opencypher.okapi.impl.util.ScalaUtils._

object GraphDdlConversions {

  private val NO_LABEL = "NO_LABEL"

  implicit class SchemaOps(schema: Schema) {

    def asGraphType: GraphType = {
      val allKeys = schema.nodeKeys ++ schema.relationshipKeys

      val nodeElementTypes = schema.labelCombinations.combos.flatMap { labelCombo =>
        val comboProperties = schema.nodePropertyKeys(labelCombo)
        if (labelCombo.isEmpty) {
          Set(NO_LABEL -> comboProperties)
        } else {
          labelCombo.map(_ -> comboProperties)
        }
      }
      val relElementTypes = schema.relationshipTypes.map { relType =>
        relType -> schema.relationshipPropertyKeys(relType)
      }

      val flatElementTypes = (nodeElementTypes ++ relElementTypes)
        .groupBy { case (name, _) => name }
        .map { case (name, keySets) =>
          val propertyKeysSets = keySets.map(_._2.toSet)
          val intersect = propertyKeysSets.reduce(_ intersect _)
          val union = propertyKeysSets.map(_ -- intersect).reduce(_ union _)
          val unionOptional = union.map { case (key, ct) => key -> ct.nullable }
          val propertyKeys = intersect ++ unionOptional

          name -> propertyKeys.toMap
        }
        .toSeq

      val explicitInheritance = extractInheritance(flatElementTypes)
      val implicitInheritance = explicitInheritance.map { case (label, (parents, propertyKeys)) =>
        val superTypes = parents.filterNot { parent =>
          val otherParents = parents - parent
          otherParents.exists(otherParent => explicitInheritance(otherParent)._1.contains(parent))
        }
        label -> (superTypes -> propertyKeys)
      }
      val elementTypes = implicitInheritance.toSeq
        .sortBy { case (_, (parents, _)) => parents.size }
        .map { case (label, (parents, propertyKeys)) =>
          val maybeKey = allKeys.get(label) match {
            case Some(s) => Some(label -> s)
            case None => None
          }
          ElementType(label, parents, propertyKeys, maybeKey)
        }

      GraphType.empty
        .foldLeftOver(elementTypes) { case (graphType, elementType) =>
          graphType.withElementType(elementType)
        }
        .foldLeftOver(schema.labelCombinations.combos) { case (graphType, labelCombo) =>
          val nodeLabels = if (labelCombo.isEmpty) Set(NO_LABEL) else labelCombo
          graphType.withNodeType(nodeLabels.toSeq: _*)
        }
        .foldLeftOver(schema.schemaPatterns) { case (graphType, pattern) =>
          graphType.withRelationshipType(pattern.sourceLabelCombination, Set(pattern.relType), pattern.targetLabelCombination)
        }
    }

    private def extractInheritance(
      elementTypes: Seq[(String, PropertyKeys)],
      output: Map[String, (Set[String], PropertyKeys)] = Map.empty
    ): Map[String, (Set[String], PropertyKeys)] = {
      if (elementTypes.isEmpty) {
        output
      } else {

        val sortedElementTypes = elementTypes.sortBy { case (name, keys) => (name, keys.size) }

        val (label, propertyKeys) = sortedElementTypes.head
        val propertyKeysSet = propertyKeys.toSet.filterNot(_._2.isNullable)

        val updatedOutput = sortedElementTypes.foldLeft(output) {

          case (currentOutput, (currentLabel, _)) if currentLabel == label && currentOutput.contains(currentLabel) =>
            currentOutput

          case (currentOutput, (currentLabel, currentPropertyKeys)) if currentLabel == label =>
            currentOutput.updated(currentLabel, Set.empty[String] -> currentPropertyKeys)

          case (currentOutput, (currentLabel, currentPropertyKeys)) =>
            val currentPropertyKeysSet = currentPropertyKeys.toSet
            val updatedPropertyKeys = if (propertyKeysSet.subsetOf(currentPropertyKeysSet)) {
              (currentPropertyKeysSet -- propertyKeysSet).toMap
            } else {
              currentPropertyKeys
            }
            val isSubType = updatedPropertyKeys.size < currentPropertyKeys.size

            val currentParents = currentOutput.get(currentLabel) match {
              case Some((parents, _)) => parents
              case None => Set.empty[String]
            }
            val updatedParents = if (isSubType) {
              currentParents + label
            } else {
              currentParents
            }
            currentOutput.updated(currentLabel, updatedParents -> updatedPropertyKeys)
        }

        val remainingElementTypes = sortedElementTypes.tail.map { case (name, _) => name -> updatedOutput(name)._2 }

        extractInheritance(remainingElementTypes, updatedOutput)
      }
    }
  }

  implicit class GraphTypeOps(graphType: GraphType) {

    lazy val allPatterns: Set[SchemaPattern] =
      graphType.relTypes.map(edgeType => SchemaPattern(
        sourceLabelCombination = edgeType.startNodeType.labels,
        // TODO: validate there is only one rel type
        relType = edgeType.labels.head,
        targetLabelCombination = edgeType.endNodeType.labels
      ))

    // TODO move out of Graph DDL (maybe to CAPSSchema)
    def asOkapiSchema: Schema = Schema.empty
      .foldLeftOver(graphType.nodeTypes) { case (schema, nodeType) =>
        val combo = if (nodeType.labels.contains(NO_LABEL)) Set.empty[String] else nodeType.labels
        schema.withNodePropertyKeys(combo, graphType.nodePropertyKeys(nodeType))
      }
      .foldLeftOver(graphType.nodeElementTypes) { case (schema, eType) =>
        eType.maybeKey.fold(schema)(key => schema.withNodeKey(eType.name, key._2))
      }
      // TODO: validate there is only one rel type
      .foldLeftOver(graphType.relTypes) { case (schema, relType) =>
      schema.withRelationshipPropertyKeys(relType.labels.head, graphType.relationshipPropertyKeys(relType))
    }
      // TODO: validate there is only one rel type
      .foldLeftOver(graphType.relElementTypes) { case (schema, eType) =>
      eType.maybeKey.fold(schema)(key => schema.withNodeKey(eType.name, key._2))
    }
      .withSchemaPatterns(allPatterns.toSeq: _*)
  }
}
