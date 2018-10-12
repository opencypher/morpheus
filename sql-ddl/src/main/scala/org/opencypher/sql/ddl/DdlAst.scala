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
import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.trees.AbstractTreeNode
import org.opencypher.sql.ddl.Ddl._

object Ddl {
  type Property = (String, CypherType)

  type EntityDefinition = (String, Map[String, CypherType])

  type KeyDefinition = (String, Set[String])

  type ColumnIdentifier = List[String]

  type PropertyToColumnMappingDefinition = Map[String, String]
}

abstract class DdlAst extends AbstractTreeNode[DdlAst]

case class DdlDefinitions(
  labelDefinitions: List[LabelDefinition] = Nil,
  schemaDefinitions: Map[String, SchemaDefinition] = Map.empty,
  graphDefinitions: List[GraphDefinition] = Nil,
  nodeMappings: List[NodeMappingDefinition] = Nil,
  relationshipLabelSets: List[RelationshipLabelSetDefinition] = Nil
) extends DdlAst {

  private lazy val globalLabelDefinitions: Map[String, LabelDefinition] = labelDefinitions.map(labelDef => labelDef.name -> labelDef).toMap

  private lazy val globalSchemas: Map[String, Schema] = schemaDefinitions.map {
    case (name, schemaDefinition: SchemaDefinition) => name -> toSchema(schemaDefinition)
  }

  private def toSchema(schemaDefinition: SchemaDefinition): Schema = {
    val labelDefinitions = globalLabelDefinitions ++ schemaDefinition.localLabelDefinitions.map(labelDef => labelDef.name -> labelDef).toMap

    def undefinedLabelException(label: String) = IllegalArgumentException(s"Defined label (one of: ${labelDefinitions.keys.mkString("[", ", ", "]")})", label)

    // Nodes

    val schemaWithNodes = schemaDefinition.nodeDefinitions.foldLeft(Schema.empty) {
      case (currentSchema, labelCombo) =>
        val comboProperties = labelCombo.foldLeft(PropertyKeys.empty) { case (currProps, label) =>
          currProps ++ labelDefinitions.getOrElse(label, throw undefinedLabelException(label)).properties
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

    val schemaWithRels = schemaDefinition.relDefinitions.foldLeft(schemaWithNodeKeys) {
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

    val schemaWithPatterns = schemaDefinition.schemaPatternDefinitions.foldLeft(schemaWithRelationshipKeys) {
      // TODO: extend OKAPI schema with cardinality constraints
      case (currentSchema, SchemaPatternDefinition(sourceLabels, _, relTypes, _, targetLabels)) =>
        relTypes.foldLeft(currentSchema) {
          case (innerSchema, relType) =>
            innerSchema.withSchemaPatterns(SchemaPattern(sourceLabels, relType, targetLabels))
        }
    }
    schemaWithPatterns
  }

  lazy val graphSchemas: Map[String, Schema] = graphDefinitions.map {
    case GraphDefinition(name, maybeSchemaName, localSchemaDefinition, _) =>
      val globalSchema = maybeSchemaName match {
        case Some(schemaName) => globalSchemas(schemaName)
        case None => Schema.empty
      }
      val localSchema = toSchema(localSchemaDefinition)
      name -> (globalSchema ++ localSchema)
  }.toMap

}

case class LabelDefinition(
  name: String,
  properties: Map[String, CypherType] = Map.empty,
  maybeKeyDefinition: Option[KeyDefinition] = None
) extends DdlAst

case class SchemaDefinition(
  localLabelDefinitions: Set[LabelDefinition] = Set.empty,
  nodeDefinitions: Set[Set[String]] = Set.empty,
  relDefinitions: Set[String] = Set.empty,
  schemaPatternDefinitions: Set[SchemaPatternDefinition] = Set.empty
) extends DdlAst

case class GraphDefinition(
  name: String,
  maybeSchemaName: Option[String] = None,
  localSchemaDefinition: SchemaDefinition = SchemaDefinition(),
  nodeMappings: List[NodeMappingDefinition] = List.empty
) extends DdlAst

case class CardinalityConstraint(from: Int, to: Option[Int])

case class SchemaPatternDefinition(
  sourceLabels: Set[String],
  sourceCardinality: CardinalityConstraint,
  relTypes: Set[String],
  targetCardinality: CardinalityConstraint,
  targetLabels: Set[String]
) extends DdlAst

case class NodeMappingDefinition(
  labelNames: Set[String],
  viewName: String,
  maybePropertyMapping: Option[PropertyToColumnMappingDefinition] = None
) extends DdlAst

case class SourceViewDefinition(name: String, alias: String) extends DdlAst

case class JoinOnDefinition(joinPredicates: List[(ColumnIdentifier, ColumnIdentifier)]) extends DdlAst

case class LabelToViewDefinition(
  labelSet: Set[String],
  sourceView: SourceViewDefinition,
  joinOn: JoinOnDefinition
) extends DdlAst

case class RelationshipMappingDefinition(
  sourceView: SourceViewDefinition,
  startNodeMappingDefinition: LabelToViewDefinition,
  endNodeMappingDefinition: LabelToViewDefinition
) extends DdlAst

case class RelationshipLabelSetDefinition(
  relType: String,
  relationshipMappings: List[RelationshipMappingDefinition]
) extends DdlAst
