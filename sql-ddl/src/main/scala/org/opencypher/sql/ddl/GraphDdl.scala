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
package org.opencypher.sql.ddl

import org.opencypher.okapi.api.schema.{PropertyKeys, Schema, SchemaPattern}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, SchemaException}

case class GraphDdl(ddl: DdlDefinition) {

  private[ddl] lazy val globalLabelDefinitions: Map[String, LabelDefinition] =
    ddl.labelDefinitions.map(labelDef => labelDef.name -> labelDef).toMap

  lazy val graphByName: Map[String, GraphDefinition] =
    ddl.graphDefinitions.map { graphDef => graphDef.name -> graphDef }.toMap

  private[ddl] lazy val globalSchemas: Map[String, Schema] =
    ddl.schemaDefinitions.map { case (name, schemaDefinition: SchemaDefinition) => name -> toSchema(schemaDefinition) }

  lazy val graphSchemas: Map[String, Schema] = ddl.graphDefinitions.map {
    case GraphDefinition(name, maybeSchemaName, localSchemaDefinition, _, _) =>
      val globalSchema = maybeSchemaName match {
        case Some(schemaName) => globalSchemas(schemaName)
        case None => Schema.empty
      }
      val localSchema = toSchema(localSchemaDefinition)
      name -> (globalSchema ++ localSchema)
  }.toMap

  private def toSchema(schemaDefinition: SchemaDefinition): Schema = {
    val labelDefinitions = globalLabelDefinitions ++ schemaDefinition.localLabelDefinitions.map(labelDef => labelDef.name -> labelDef).toMap

    def undefinedLabelException(label: String) = IllegalArgumentException(s"Defined label (one of: ${labelDefinitions.keys.mkString("[", ", ", "]")})", label)

    // track all node / rel definitions (e.g. explicit ones and implicit ones from schema pattern definitions)
    val nodeDefinitionsFromPatterns = schemaDefinition.schemaPatternDefinitions.flatMap(schemaDef =>
      schemaDef.sourceLabelCombinations ++ schemaDef.targetLabelCombinations)
    val relDefinitionsFromPatterns = schemaDefinition.schemaPatternDefinitions.flatMap(_.relTypes)
    // Nodes

    val schemaWithNodes = (nodeDefinitionsFromPatterns ++ schemaDefinition.nodeDefinitions).foldLeft(Schema.empty) {
      case (currentSchema, labelCombo) =>
        val comboProperties = labelCombo.foldLeft(PropertyKeys.empty) { case (currProps, label) =>
          val labelProperties = labelDefinitions.getOrElse(label, throw undefinedLabelException(label)).properties
          currProps.keySet.intersect(labelProperties.keySet).foreach { key =>
            if (currProps(key) != labelProperties(key)) {
              throw SchemaException(
                s"""|Incompatible property types for label combination (${labelCombo.mkString(",")})
                    |Property key `$key` has conflicting types ${currProps(key)} and ${labelProperties(key)}
                 """.stripMargin)
            }
          }
          currProps ++ labelProperties
        }
        currentSchema.withNodePropertyKeys(labelCombo, comboProperties)
    }

    val usedLabels = schemaDefinition.nodeDefinitions.flatten
    val schemaWithNodeKeys = usedLabels.foldLeft(schemaWithNodes) {
      case (currentSchema, label) =>
        labelDefinitions(label).maybeKeyDefinition match {
          case Some((_, keys)) => currentSchema.withNodeKey(label, keys)
          case None => currentSchema
        }
    }

    // Relationships

    val schemaWithRels = (relDefinitionsFromPatterns ++ schemaDefinition.relDefinitions).foldLeft(schemaWithNodeKeys) {
      case (currentSchema, relType) =>
        currentSchema.withRelationshipPropertyKeys(relType, labelDefinitions.getOrElse(relType, throw undefinedLabelException(relType)).properties)
    }

    val usedRelTypes = schemaDefinition.relDefinitions
    val schemaWithRelationshipKeys = usedRelTypes.foldLeft(schemaWithRels) {
      case (currentSchema, relType) =>
        labelDefinitions(relType).maybeKeyDefinition match {
          case Some((_, keys)) => currentSchema.withRelationshipKey(relType, keys)
          case None => currentSchema
        }
    }

    // Schema patterns

    schemaDefinition.schemaPatternDefinitions.foldLeft(schemaWithRelationshipKeys) {
      // TODO: extend OKAPI schema with cardinality constraints
      case (currentSchema, SchemaPatternDefinition(sourceLabelCombinations, _, relTypes, _, targetLabelCombinations)) =>
        val expandedPatterns = for {
          sourceCombination <- sourceLabelCombinations
          relType <- relTypes
          targetLabelCombination <- targetLabelCombinations
        } yield SchemaPattern(sourceCombination, relType, targetLabelCombination)
        currentSchema.withSchemaPatterns(expandedPatterns.toSeq: _*)
    }
  }
}
