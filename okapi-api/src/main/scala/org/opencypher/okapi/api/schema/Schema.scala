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
package org.opencypher.okapi.api.schema

import org.opencypher.okapi.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.okapi.api.types.{CTRelationship, CypherType}
import org.opencypher.okapi.impl.schema.SchemaImpl._
import org.opencypher.okapi.impl.schema.{ImpliedLabels, LabelCombinations, SchemaImpl}

object Schema {
  /**
    * Empty Schema. Start with this to construct a new Schema.
    * Use the `with*` functions to add information.
    */
  val empty: Schema = SchemaImpl(
    labelPropertyMap = LabelPropertyMap.empty,
    relTypePropertyMap = RelTypePropertyMap.empty
  )

  def fromJson(jsonString: String): Schema =
    upickle.default.read[Schema](jsonString)
}

/**
  * The schema of a graph describes what labels and relationship types exist in a graph,
  * including possible combinations of the former.
  * It also keeps track of properties and their types that appear on different labels, label combinations and
  * relationship types.
  */
trait Schema {
  /**
    * All labels present in this graph.
    */
  def labels: Set[String]

  /**
    * All relationship types present in this graph.
    */
  def relationshipTypes: Set[String]

  /**
    * Property keys and their types for node labels.
    */
  def labelPropertyMap: LabelPropertyMap

  /**
    * Property keys and their types for relationship types.
    */
  def relTypePropertyMap: RelTypePropertyMap

  /**
    * Implied labels for each existing label.
    */
  def impliedLabels: ImpliedLabels

  /**
    * Groups of labels where each group contains possible label combinations.
    */
  def labelCombinations: LabelCombinations

  /**
    * Given a set of labels that a node definitely has, returns all labels the node _must_ have.
    */
  def impliedLabels(knownLabels: Set[String]): Set[String]

  /**
    * Given a set of labels that a node definitely has, returns all labels the node _must_ have.
    */
  def impliedLabels(knownLabels: String*): Set[String] = {
    impliedLabels(knownLabels.toSet)
  }

  /**
    * Given a set of labels that a node definitely has, returns its property schema.
    *
    * TODO: consider implied labels here?
    */
  def nodeKeys(labels: Set[String]): PropertyKeys

  /**
    * Given a set of labels that a node definitely has, returns its property schema.
    */
  def nodeKeys(labels: String*): PropertyKeys = {
    nodeKeys(labels.toSet)
  }

  /**
    * Returns the property schema of a node of which the labels are unknown.
    */
  def allNodeKeys: PropertyKeys

  /**
    * Returns all combinations of labels that exist on a node in the graph.
    */
  def allLabelCombinations: Set[Set[String]]

  /**
    * Given a set of labels that a node definitely has, returns all combinations of labels that the node could possibly have.
    */
  def combinationsFor(knownLabels: Set[String]): Set[Set[String]]

  /**
    * Returns some property type for a property given the labels of a node.
    * Returns none if this property does not appear on nodes with the given label combination.
    * Types of conflicting property keys are joined.
    *
    * @param labels label combination for which the property type is checked
    * @param key    property key
    * @return Cypher type of the property on nodes with the given label combination
    */
  def nodeKeyType(labels: Set[String], key: String): Option[CypherType]

  /**
    * Returns property keys for the set of label combinations.
    * Types of conflicting property keys are joined.
    *
    * @param labelCombinations label combinations to consider
    * @return typed property keys, with joined or nullable types for conflicts
    */
  def keysFor(labelCombinations: Set[Set[String]]): PropertyKeys

  /**
    * Returns the property keys that exist on any of the labels.
    * Types of conflicting property keys are joined.
    *
    * @param labels labels for which to return the property keys
    * @return property keys for labels
    */
  def keysFor(labels: String*): PropertyKeys= {
    keysFor(labels.map(Set(_)).toSet)
  }

  /**
    * Returns some property type for a property given the possible types of a relationship.
    * Returns none if this property does not appear on relationships with one of the given types.
    * Types of conflicting property keys are joined.
    *
    * @param types relationship types for which the property type is checked
    * @param key    property key
    * @return Cypher type of the property on relationships with one of the given types
    */
  def relationshipKeyType(types: Set[String], key: String): Option[CypherType]

  /**
    * Returns the property schema for a given relationship type
    */
  def relationshipKeys(typ: String): PropertyKeys

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
  def withNodePropertyKeys(nodeLabels: Set[String], keys: PropertyKeys = PropertyKeys.empty): Schema

  /**
    * @see [[org.opencypher.okapi.api.schema.Schema#withNodePropertyKeys(scala.collection.Seq, scala.collection.Seq)]]
    */
  def withNodePropertyKeys(nodeLabels: String*)(keys: (String, CypherType)*): Schema =
    withNodePropertyKeys(nodeLabels.toSet, keys.toMap)

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
  def withRelationshipPropertyKeys(typ: String, keys: PropertyKeys): Schema

  /**
    * @see [[org.opencypher.okapi.api.schema.Schema#withRelationshipPropertyKeys(java.lang.String, scala.collection.Seq)]]
    */
  def withRelationshipType(relType: String): Schema =
    withRelationshipPropertyKeys(relType)()

  /**
    * @see [[org.opencypher.okapi.api.schema.Schema#withRelationshipPropertyKeys(java.lang.String, scala.collection.Seq)]]
    */
  def withRelationshipPropertyKeys(typ: String)(keys: (String, CypherType)*): Schema =
    withRelationshipPropertyKeys(typ, keys.toMap)

  /**
    * Returns the union of the input schemas.
    * Conflicting property key types are resolved into the joined type.
    */
  def ++(other: Schema): Schema

  /**
    * Returns this schema with the properties for `combo` removed.
    * @param combo label combination for which properties are removed
    * @return updated schema
    */
  private[opencypher] def dropPropertiesFor(combo: Set[String]): Schema

  /**
    * Returns the sub-schema for a node scan under the given constraints.
    * Labels are interpreted as constraints on the resulting Schema.
    * If no labels are specified, this means the resulting node can have any valid label combination.
    *
    * @param labelConstraints Specifies the labels that the node is guaranteed to have
    * @return sub-schema for `labelConstraints`
    */
  private[opencypher] def forNode(labelConstraints: Set[String]): Schema

  /**
    * Returns the sub-schema for `relType`
    *
    * @param relType Specifies the type for which the schema is extracted
    * @return sub-schema for `relType`
    */
  private[opencypher] def forRelationship(relType: CTRelationship): Schema

  /**
    * Returns the updated schema, but overwrites any existing node property keys for the given labels.
    */
  private[opencypher] def withOverwrittenNodePropertyKeys(nodeLabels: Set[String], propertyKeys: PropertyKeys): Schema

  /**
    * Returns the updated schema, but overwrites any existing relationship property keys for the given type.
    */
  private[opencypher] def withOverwrittenRelationshipPropertyKeys(relType: String, propertyKeys: PropertyKeys): Schema

  def toString: String

  def toJson: String

  def pretty: String

  def isEmpty: Boolean
}
