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
}

abstract class DdlAst extends AbstractTreeNode[DdlAst]

case class DdlDefinitions(
  labelDefinitions: List[LabelDefinition] = List.empty,
  schemaDefinitions: Map[String, SchemaDefinition] = Map.empty,
  graphDefinitions: List[GraphDefinition] = List.empty
) extends DdlAst {

  lazy val globalLabelDefinitions: Map[String, LabelDefinition] = labelDefinitions.map(labelDef => labelDef.name -> labelDef).toMap

  lazy val globalSchemas: Map[String, Schema] = {
    schemaDefinitions.map { case (name, SchemaDefinition(localLabelDefinitions, nodeDefinitions, relDefinitions, schemaPatternDefinitions)) =>
      val labelDefinitions = globalLabelDefinitions ++ localLabelDefinitions.map(labelDef => labelDef.name -> labelDef).toMap
      def undefinedLabelException(label: String) = IllegalArgumentException(s"Defined label (one of: ${labelDefinitions.keys.mkString("[", ", ", "]")})", label)

      val schemaWithNodes = nodeDefinitions.foldLeft(Schema.empty) {
        case (currentSchema, labelCombo) =>
          val comboProperties = labelCombo.foldLeft(PropertyKeys.empty) { case (currProps, label) =>
            currProps ++ labelDefinitions.getOrElse(label, throw undefinedLabelException(label)).properties
          }
          currentSchema.withNodePropertyKeys(labelCombo, comboProperties)
      }

      val schemaWithRels = relDefinitions.foldLeft(schemaWithNodes) {
        case (currentSchema, relType) =>
          currentSchema.withRelationshipPropertyKeys(relType, labelDefinitions.getOrElse(relType, throw undefinedLabelException(relType)).properties)
      }

      val schemaWithPatterns = schemaPatternDefinitions.foldLeft(schemaWithRels) {
        // TODO: extend OKAPI schema with cardinality constraints
        case (currentSchema, SchemaPatternDefinition(sourceLabels, _, relTypes, _, targetLabels)) =>
          relTypes.foldLeft(currentSchema) {
            case (innerSchema, relType) =>
              innerSchema.withSchemaPatterns(SchemaPattern(sourceLabels, relType, targetLabels))
          }
      }
      name -> schemaWithPatterns
    }
  }

  //  val schemas: Map[String, Schema] = {
  //    graphDefinitions.map { graphDef =>
  //      val name = graphDef.name
  //      val globalSchemaDef: Option[SchemaDefinition] = graphDef.schemaName
  //    }
  //  }

}

case class LabelDefinition(
  name: String,
  properties: Map[String, CypherType] = Map.empty,
  keyDefinition: Option[KeyDefinition] = None
) extends DdlAst

case class SchemaDefinition(
  localLabelDefinitions: Set[LabelDefinition] = Set.empty,
  nodeDefinitions: Set[Set[String]] = Set.empty,
  relDefinitions: Set[String] = Set.empty,
  schemaPatternDefinitions: Set[SchemaPatternDefinition] = Set.empty
) extends DdlAst

case class GraphDefinition(
  name: String,
  schemaName: Option[String] = None,
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
  viewName: String
) extends DdlAst

case class RelMappingDefinition(
  relName: String,
  tableName: String,
  startNodeIdMappings: List[IdMapping],
  endNodeIdMappings: List[IdMapping]
) extends DdlAst

case class IdMapping(
  labelNames: Set[String],
  nodePropertyNames: List[String],
  relPropertyNames: List[String]
)
