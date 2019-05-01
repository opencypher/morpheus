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
import org.opencypher.okapi.api.types.CTRelationship
import org.opencypher.okapi.impl.exception.IllegalArgumentException

object RelationshipMappingBuilder {

  /**
    * @param sourceIdKey represents a key to the relationship identifier within the source data. The retrieved value
    *                    from the source data is expected to be a [[scala.Long]] value that is unique among relationships.
    * @return incomplete relationship mapping
    */
  def withSourceIdKey(sourceIdKey: String): MissingSourceStartNodeKey =
    new MissingSourceStartNodeKey(sourceIdKey)

  /**
    * Alias for [[org.opencypher.okapi.api.io.conversion.RelationshipMappingBuilder#withSourceIdKey]].
    *
    * @param sourceIdKey represents a key to the relationship identifier within the source data. The retrieved value
    *                    from the source data is expected to be a [[scala.Long]] value that is unique among relationships.
    * @return incomplete relationship mapping
    */
  def on(sourceIdKey: String): MissingSourceStartNodeKey =
    withSourceIdKey(sourceIdKey)

  /**
    * Creates a RelationshipMapping where property keys match with their corresponding keys in the source data.
    *
    * See [[org.opencypher.okapi.api.io.conversion.RelationshipMappingBuilder]] for further information.
    *
    * @param sourceIdKey        key to access the node identifier in the source data
    * @param sourceStartNodeKey key to access the start node identifier in the source data
    * @param sourceEndNodeKey   key to access the end node identifier in the source data
    * @param relType            relationship type
    * @param properties         property keys
    * @return relationship mapping
    */
  def create(
    sourceIdKey: String,
    sourceStartNodeKey: String,
    sourceEndNodeKey: String,
    relType: String,
    properties: Set[String] = Set.empty
  ): ElementMapping = {
    val intermediateMapping = RelationshipMappingBuilder
      .withSourceIdKey(sourceIdKey)
      .withSourceStartNodeKey(sourceStartNodeKey)
      .withSourceEndNodeKey(sourceEndNodeKey)
      .withRelType(relType)

    properties.foldLeft(intermediateMapping) {
      (mapping, property) => mapping.withPropertyKey(property)
    }.build
  }

  sealed class MissingSourceStartNodeKey(sourceIdKey: String) {
    /**
      * @param sourceStartNodeKey represents a key to the start node identifier within the source data. The retrieved
      *                           value from the source data is expected to be a [[scala.Long]] value.
      * @return incomplete relationship mapping builder
      */
    def withSourceStartNodeKey(sourceStartNodeKey: String): MissingSourceEndNodeKey =
      new MissingSourceEndNodeKey(sourceIdKey, sourceStartNodeKey)

    /**
      * Alias for [[org.opencypher.okapi.api.io.conversion.RelationshipMappingBuilder.MissingSourceStartNodeKey#withSourceStartNodeKey]].
      *
      * @param sourceStartNodeKey represents a key to the start node identifier within the source data. The retrieved
      *                           value from the source data is expected to be a [[scala.Long]] value.
      * @return incomplete relationship mapping builder
      */
    def from(sourceStartNodeKey: String): MissingSourceEndNodeKey =
      withSourceStartNodeKey(sourceStartNodeKey)
  }

  sealed class MissingSourceEndNodeKey(sourceIdKey: String, sourceStartNodeKey: String) {
    /**
      * @param sourceEndNodeKey represents a key to the end node identifier within the source data. The retrieved
      *                         value from the source data is expected to be a [[scala.Long]] value.
      * @return incomplete relationship mapping builder
      */
    def withSourceEndNodeKey(sourceEndNodeKey: String): MissingRelTypeMapping =
      new MissingRelTypeMapping(sourceIdKey, sourceStartNodeKey, sourceEndNodeKey)

    /**
      * Alias for [[withSourceEndNodeKey]].
      *
      * @param sourceEndNodeKey represents a key to the end node identifier within the source data. The retrieved
      *                         value from the source data is expected to be a [[scala.Long]] value.
      * @return incomplete relationship mapping builder
      */
    def to(sourceEndNodeKey: String): MissingRelTypeMapping =
      withSourceEndNodeKey(sourceEndNodeKey)
  }

  sealed class MissingRelTypeMapping(sourceIdKey: String, sourceStartNodeKey: String, sourceEndNodeKey: String) {
    /**
      * @param relType represents the relationship type for all relationships in the source data
      * @return relationship mapping builder
      */
    def withRelType(relType: String): RelationshipMappingBuilder =
      RelationshipMappingBuilder(sourceIdKey, sourceStartNodeKey, sourceEndNodeKey, relType)

    /**
      * Alias for [[withRelType]].
      *
      * @param relType represents the relationship type for all relationships in the source data
      * @return relationship mapping builder
      */
    def relType(relType: String): RelationshipMappingBuilder =
      withRelType(relType)
  }

}

/**
  * Builder to build ElementMapping with a [[NodePattern]].
  *
  * Represents a mapping from a source with key-based access to relationship components (e.g. a table definition) to a
  * Cypher relationship. The purpose of this class is to define a mapping from an external data source to a property
  * graph.
  *
  * Construct a [[RelationshipMappingBuilder]] starting with [[RelationshipMappingBuilder#on]].
  *
  * @param relationshipIdKey         key to access the node identifier in the source data
  * @param relationshipStartNodeKey  key to access the start node identifier in the source data
  * @param relationshipEndNodeKey    key to access the end node identifier in the source data
  * @param relType                   a relationship type
  * @param propertyMapping           mapping from property key to source property key
  */
final case class RelationshipMappingBuilder(
  relationshipIdKey: String,
  relationshipStartNodeKey: String,
  relationshipEndNodeKey: String,
  relType: String,
  propertyMapping: Map[String, String] = Map.empty
) extends SingleElementMappingBuilder {

  override type BuilderType = RelationshipMappingBuilder

  override protected def updatePropertyMapping(updatedPropertyMapping: Map[String, String]): RelationshipMappingBuilder =
    copy(propertyMapping = updatedPropertyMapping)

  override def build: ElementMapping = {
    validate()

    val pattern: RelationshipPattern = RelationshipPattern(CTRelationship(relType))

    val properties: Map[PatternElement, Map[String, String]] = Map(pattern.relElement -> propertyMapping)
    val idKeys: Map[PatternElement, Map[IdKey, String]] = Map(
      pattern.relElement -> Map(
        SourceIdKey -> relationshipIdKey,
        SourceStartNodeKey -> relationshipStartNodeKey,
        SourceEndNodeKey -> relationshipEndNodeKey
      )
    )

    ElementMapping(pattern, properties, idKeys)
  }

  override protected def validate(): Unit = {
    val idKeys = Set(relationshipIdKey, relationshipStartNodeKey, relationshipEndNodeKey)

    if (idKeys.size != 3)
      throw IllegalArgumentException(
        s"id ($relationshipIdKey), start ($relationshipStartNodeKey) and end ($relationshipEndNodeKey) source keys need to be distinct",
        s"non-distinct source keys")

  }
}
