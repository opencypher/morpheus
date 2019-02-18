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
package org.opencypher.okapi.api.io.conversion

import org.opencypher.okapi.api.graph._
import org.opencypher.okapi.api.types.CTNode

object NodeMapping {
  /**
    * Alias for [[withSourceIdKey]].
    *
    * @param sourceIdKey key to access the node identifier in the source data
    * @return node mapping
    */
  def on(sourceIdKey: String): NodeMappingBuilder =
    withSourceIdKey(sourceIdKey)

  /**
    *
    * @param sourceIdKey represents a key to the node identifier within the source data. The retrieved value
    *                    from the source data is expected to be a [[Long]] value that is unique among nodes.
    * @return node mapping
    */
  def withSourceIdKey(sourceIdKey: String): NodeMappingBuilder =
    NodeMappingBuilder(sourceIdKey)

  /**
    * Creates a NodeMapping where optional labels and property keys match with their corresponding keys in the source
    * data.
    *
    * See [[NodeMapping]] for further information.
    *
    * @param nodeIdKey      key to access the node identifier in the source data
    * @param impliedLabels  set of node labels
    * @param propertyKeys   set of property keys
    * @return node mapping
    */
  def create(
    nodeIdKey: String,
    impliedLabels: Set[String] = Set.empty,
    propertyKeys: Set[String] = Set.empty
  ): EntityMapping = {

    val mappingWithImpliedLabels = impliedLabels.foldLeft(NodeMapping.withSourceIdKey(nodeIdKey)) {
      (mapping, label) => mapping.withImpliedLabel(label)
    }

    propertyKeys.foldLeft(mappingWithImpliedLabels) {
      (mapping, property) => mapping.withPropertyKey(property)
    }.build
  }
}

///**
//  * Represents a mapping from a source with key-based access to node components (e.g. a table definition) to a Cypher
//  * node. The purpose of this class is to define a mapping from an external data source to a property graph.
//  *
//  * Construct a [[NodeMapping]] starting with [[NodeMapping#on]].
//  *
//  * The [[nodeIdKey]] represents a key to the node identifier within the source data. The retrieved value from the
//  * source data is expected to be a [[scala.Long]] value that is unique among nodes.
//  *
//  * The [[impliedNodeLabels]] represent a set of node labels.
//  *
//  * The [[optionalNodeLabelMapping]] represent a map from node labels to keys in the source data. The retrieved value from
//  * the source data is expected to be a [[scala.Boolean]] value indicating if the label is present on that node.
//  *
//  * The [[nodePropertyMapping]] represents a map from node property keys to keys in the source data. The retrieved value
//  * from the source is expected to be convertible to a valid [[org.opencypher.okapi.api.value.CypherValue]].
//  *
//  * @param nodeIdKey          key to access the node identifier in the source data
//  * @param impliedNodeLabels        set of node labels
//  * @param optionalNodeLabelMapping mapping from label to source key
//  * @param nodePropertyMapping      mapping from property key to source property key
//  */
final case class NodeMappingBuilder(
  nodeIdKey: String,
  impliedNodeLabels: Set[String] = Set.empty,
  propertyMapping: Map[String, String] = Map.empty
) extends EntityMappingBuilder {

  override type BuilderType = NodeMappingBuilder

  def withImpliedLabels(labels: String*): NodeMappingBuilder =
    labels.foldLeft(this)((mapping, label) => mapping.withImpliedLabel(label))

  def withImpliedLabel(label: String): NodeMappingBuilder =
    copy(impliedNodeLabels = impliedNodeLabels + label)

  override protected def updatePropertyMapping(updatedPropertyMapping: Map[String, String]): NodeMappingBuilder =
    copy(propertyMapping = updatedPropertyMapping)

  override def build: EntityMapping = {
    val pattern: NodePattern = NodePattern(CTNode(impliedNodeLabels))
    val properties: Map[Entity, Map[String, String]] = Map(pattern.nodeEntity -> propertyMapping)
    val idKeys: Map[Entity, Map[IdKey, String]] = Map(pattern.nodeEntity -> Map(SourceIdKey -> nodeIdKey))
    val impliedTypes: Map[Entity, Set[String]] = Map(pattern.nodeEntity -> impliedNodeLabels)

    validate()

    EntityMapping(pattern, properties, idKeys, impliedTypes)
  }

  override protected def validate(): Unit = ()
}
