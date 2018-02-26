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
package org.opencypher.caps.api.io.conversion

import org.opencypher.caps.api.types.CTRelationship
import org.opencypher.caps.impl.exception.IllegalArgumentException

object RelationshipMapping {

  /**
    * @param sourceIdKey represents a key to the relationship identifier within the source data. The retrieved value
    *                    from the source data is expected to be a [[scala.Long]] value that is unique among relationships.
    * @return incomplete relationship mapping
    */
  def withSourceIdKey(sourceIdKey: String): MissingSourceStartNodeKey =
    new MissingSourceStartNodeKey(sourceIdKey)

  /**
    * Alias for [[org.opencypher.caps.api.io.conversion.RelationshipMapping#withSourceIdKey]].
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
    * See [[org.opencypher.caps.api.io.conversion.RelationshipMapping]] for further information.
    *
    * @param sourceIdKey        key to access the node identifier in the source data
    * @param sourceStartNodeKey key to access the start node identifier in the source data
    * @param sourceEndNodeKey   key to access the end node identifier in the source data
    * @param relType            relationship type
    * @param properties         property keys
    * @return relationship mapping
    */
  def create(sourceIdKey: String, sourceStartNodeKey: String, sourceEndNodeKey: String, relType: String, properties: Set[String] = Set.empty): RelationshipMapping = {
    val intermediateMapping = RelationshipMapping
      .withSourceIdKey(sourceIdKey)
      .withSourceStartNodeKey(sourceStartNodeKey)
      .withSourceEndNodeKey(sourceEndNodeKey)
      .withRelType(relType)

    properties.foldLeft(intermediateMapping) {
      (mapping, property) => mapping.withPropertyKey(property)
    }
  }

  sealed class MissingSourceStartNodeKey(sourceIdKey: String) {
    /**
      * @param sourceStartNodeKey represents a key to the start node identifier within the source data. The retrieved
      *                           value from the source data is expected to be a [[scala.Long]] value.
      * @return incomplete relationship mapping
      */
    def withSourceStartNodeKey(sourceStartNodeKey: String): MissingSourceEndNodeKey =
      new MissingSourceEndNodeKey(sourceIdKey, sourceStartNodeKey)

    /**
      * Alias for [[org.opencypher.caps.api.io.conversion.RelationshipMapping.MissingSourceStartNodeKey#withSourceStartNodeKey]].
      *
      * @param sourceStartNodeKey represents a key to the start node identifier within the source data. The retrieved
      *                           value from the source data is expected to be a [[scala.Long]] value.
      * @return incomplete relationship mapping
      */
    def from(sourceStartNodeKey: String): MissingSourceEndNodeKey =
      withSourceStartNodeKey(sourceStartNodeKey)
  }

  sealed class MissingSourceEndNodeKey(sourceIdKey: String, sourceStartNodeKey: String) {
    /**
      * @param sourceEndNodeKey represents a key to the end node identifier within the source data. The retrieved
      *                         value from the source data is expected to be a [[scala.Long]] value.
      * @return incomplete relationship mapping
      */
    def withSourceEndNodeKey(sourceEndNodeKey: String): MissingRelTypeMapping =
      new MissingRelTypeMapping(sourceIdKey, sourceStartNodeKey, sourceEndNodeKey)

    /**
      * Alias for [[withSourceEndNodeKey()]].
      *
      * @param sourceEndNodeKey represents a key to the end node identifier within the source data. The retrieved
      *                         value from the source data is expected to be a [[scala.Long]] value.
      * @return incomplete relationship mapping
      */
    def to(sourceEndNodeKey: String): MissingRelTypeMapping =
      withSourceEndNodeKey(sourceEndNodeKey)
  }

  sealed class MissingRelTypeMapping(sourceIdKey: String, sourceStartNodeKey: String, sourceEndNodeKey: String) {
    /**
      * @param relType represents the relationship type for all relationships in the source data
      * @return relationship mapping
      */
    def withRelType(relType: String): RelationshipMapping =
      RelationshipMapping(sourceIdKey, sourceStartNodeKey, sourceEndNodeKey, Left(relType))

    /**
      * Alias for [[withRelType()]].
      *
      * @param relType represents the relationship type for all relationships in the source data
      * @return relationship mapping
      */
    def relType(relType: String): RelationshipMapping =
      withRelType(relType)

    /**
      * @param sourceRelTypeKey represents a key to the relationship type within the source data. The retrieved
      *                         value from the source data is expected to be a [[String]] value.
      * @param possibleTypes    set of possible relationship types withing the source data column
      * @return relationship mapping
      */
    def withSourceRelTypeKey(sourceRelTypeKey: String, possibleTypes: Set[String]): RelationshipMapping =
      RelationshipMapping(sourceIdKey, sourceStartNodeKey, sourceEndNodeKey, Right(sourceRelTypeKey -> possibleTypes))
  }

}

/**
  * Represents a mapping from a source with key-based access to relationship components (e.g. a table definition) to a
  * Cypher relationship. The purpose of this class is to define a mapping from an external data source to a property
  * graph.
  *
  * Construct a [[RelationshipMapping]] starting with {{RelationshipMapping.on}}.
  *
  * @param sourceIdKey               key to access the node identifier in the source data
  * @param sourceStartNodeKey        key to access the start node identifier in the source data
  * @param sourceEndNodeKey          key to access the end node identifier in the source data
  * @param relTypeOrSourceRelTypeKey either a relationship type or a key to access the type in the source data and a set of all possible types
  * @param propertyMapping           mapping from property key to source property key
  */
private[caps] final case class RelationshipMapping(
  sourceIdKey: String,
  sourceStartNodeKey: String,
  sourceEndNodeKey: String,
  relTypeOrSourceRelTypeKey: Either[String, (String, Set[String])],
  propertyMapping: Map[String, String] = Map.empty) extends EntityMapping {

  // on construction
  validate()

  def cypherType: CTRelationship = {
    val possibleRelTypes = relTypeOrSourceRelTypeKey match {
      case Left(relType) => Set(relType)
      case Right((_, possibleRelValues)) => possibleRelValues
    }
    CTRelationship(possibleRelTypes)
  }

  def withPropertyKey(propertyKey: String, sourcePropertyKey: String): RelationshipMapping = {
    preventOverwritingProperty(propertyKey)
    copy(propertyMapping = propertyMapping.updated(propertyKey, sourcePropertyKey))
  }

  def withPropertyKey(tuple: (String, String)): RelationshipMapping =
    withPropertyKey(tuple._1, tuple._2)

  def withPropertyKey(property: String): RelationshipMapping =
    withPropertyKey(property, property)

  def withPropertyKeys(properties: String*): RelationshipMapping =
    properties.foldLeft(this)((mapping, propertyKey) => mapping.withPropertyKey(propertyKey, propertyKey))

  private def validate(): Unit = {
    val idKeys = Set(sourceIdKey, sourceStartNodeKey, sourceEndNodeKey)

    if (idKeys.size != 3)
      throw IllegalArgumentException(
        s"id ($sourceIdKey, start ($sourceStartNodeKey) and end ($sourceEndNodeKey) source keys need to be distinct",
        s"non-distinct source keys")

    relTypeOrSourceRelTypeKey match {
      case Right((sourceKey, _)) if idKeys.contains(sourceKey) =>
        throw IllegalArgumentException("dedicated source column for relationship type",
          s"relationship type source column $sourceKey is referring to one of id, start or end column")
      case _ =>
    }
  }
}
