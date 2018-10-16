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

import org.opencypher.okapi.api.schema.LabelPropertyMap._
import org.opencypher.okapi.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.okapi.api.schema.RelTypePropertyMap._
import org.opencypher.okapi.api.types.{CTRelationship, CypherType}
import org.opencypher.okapi.impl.annotations.experimental
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
  * The schema of a graph describes what labels and relationship types exist in a graph, including possible combinations
  * of the former. A `label combination` is an exact set of labels on a node. The term `known labels` refers to a subset
  * of labels that a node might have.
  *
  * It also keeps track of properties and their types that appear on different labels, label combinations and
  * relationship types.
  */
trait Schema {
  /**
    * All labels present in this graph.
    */
  def labels: Set[String]

  /**
    * Returns a mapping from a node label to a set of property keys that together form an entity key for that
    * label. An entity key uniquely identifies a node with the given label.
    */
  @experimental
  def nodeKeys: Map[String, Set[String]]

  /**
    * All relationship types present in this graph.
    */
  def relationshipTypes: Set[String]

  /**
    * Returns a mapping from a relationship type to a set of property keys that together form an entity key for that
    * relationship type. An entity key uniquely identifies a relationship with the given type.
    */
  @experimental
  def relationshipKeys: Map[String, Set[String]]

  /**
    * Property keys and their types for node labels.
    */
  def labelPropertyMap: LabelPropertyMap

  /**
    * Property keys and their types for relationship types.
    */
  def relTypePropertyMap: RelTypePropertyMap

  /**
    * Returns all schema patterns known to this schema.
    * A schema pattern is a constraint over a (node)-[relationship]->(node) pattern which describes possible label and
    * relationship type combinations for the source node, the relationship and the target node in the pattern.
    *
    * If no explicit schema patterns are defined, this function will return schema patterns for all possible combinations
    * between the known label combinations and relationship types. Otherwise only explicit schema patterns will be returned.
    *
    * @see {{{org.opencypher.okapi.api.schema.SchemaPattern}}} for more information
    * @return schema pattern combinations encoded in this schema
    */
  @experimental
  def schemaPatterns: Set[SchemaPattern]

  /**
    * Retrieves the user defined schema patterns
    *
    * @return user defines schema patterns
    */
  @experimental
  def explicitSchemaPatterns: Set[SchemaPattern]

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
    * Given a set of labels that a node definitely has, returns its property keys and their types.
    */
  def nodePropertyKeys(labels: Set[String]): PropertyKeys

  /**
    * Returns some property type for a property given the known labels of a node.
    * Returns none if this property does not appear on nodes with the given label combination.
    * Types of conflicting property keys are joined.
    *
    * @param knownLabels known labels for which the property type is checked
    * @param key         property key
    * @return Cypher type of the property on nodes with the given label combination
    */
  def nodePropertyKeyType(knownLabels: Set[String], key: String): Option[CypherType]

  /**
    * Returns all combinations of labels that exist on a node in the graph.
    */
  def allCombinations: Set[Set[String]]

  /**
    * Given a set of labels that a node definitely has, returns all combinations of labels that the node could possibly have.
    */
  def combinationsFor(knownLabels: Set[String]): Set[Set[String]]

  /**
    * Returns property keys for the set of label combinations.
    * Types of conflicting property keys are joined.
    *
    * @param labelCombinations label combinations to consider
    * @return typed property keys, with joined or nullable types for conflicts
    */
  def nodePropertyKeysForCombinations(labelCombinations: Set[Set[String]]): PropertyKeys


  /**
    * Returns the property schema for a given relationship type
    */
  def relationshipPropertyKeys(relType: String): PropertyKeys

  /**
    * Returns some property type for a property given the possible types of a relationship.
    * Returns none if this property does not appear on relationships with one of the given types.
    * Types of conflicting property keys are joined.
    *
    * @param relTypes relationship types for which the property type is checked
    * @param key      property key
    * @return Cypher type of the property on relationships with one of the given types
    */
  def relationshipPropertyKeyType(relTypes: Set[String], key: String): Option[CypherType]

  /**
    * This function returns all schema patterns that are applicable with regards to the specified known labels and
    * relationship types. The given labels and relationship types are interpreted similar to how a Cypher MATCH clause
    * would interpret them. That is, the label sets are interpreted as a conjunction of label predicates, i.e. labels
    * the node must have. The relationship types are interpreted as a disjunction, i.e. the relationship must have
    * one of the given types.
    *
    * All the schema patterns that match the given descriptions will be retrieved. In particular, if all the inputs are
    * empty sets, all the schema patterns will be retrieved.
    *
    * @param knownSourceLabels labels required for the source node (AND semantics)
    * @param knownRelTypes     relationship types possible for the relationship (OR semantics)
    * @param knownTargetLabels labels required for the target node (AND semantics)
    * @return schema patterns that fulfill the predicates
    */
  @experimental
  def schemaPatternsFor(
    knownSourceLabels: Set[String],
    knownRelTypes: Set[String],
    knownTargetLabels: Set[String]
  ): Set[SchemaPattern]

  /**
    * Adds information about a label combination and its associated properties to the schema.
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
    * @param labelCombination the label combination to add to the schema
    * @param propertyKeys     the typed property keys to associate with the labels
    * @return a copy of the Schema with the provided new data
    */
  def withNodePropertyKeys(labelCombination: Set[String], propertyKeys: PropertyKeys = PropertyKeys.empty): Schema

  /**
    * @see [[org.opencypher.okapi.api.schema.Schema#withNodePropertyKeys(scala.collection.Seq, scala.collection.Seq)]]
    */
  def withNodePropertyKeys(labelCombination: String*)(propertyKeys: (String, CypherType)*): Schema =
    withNodePropertyKeys(labelCombination.toSet, propertyKeys.toMap)

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
    * Adds a node key for node label `label`. A node key uniquely identifies a node with the given label.
    *
    * @note The label for which the key is added has to exist in this schema.
    */
  @experimental
  def withNodeKey(label: String, nodeKey: Set[String]): Schema

  /**
    * Adds a relationship key for relationship type `relationshipType`. A relationship key uniquely identifies a
    * relationship with the given label.
    *
    * @note The relationship type for which the key is added has to exist in this schema.
    */
  @experimental
  def withRelationshipKey(relationshipType: String, relationshipKey: Set[String]): Schema

  /**
    * Adds the given schema patterns to the explicitly defined schema patterns.
    *
    * @note If at least one explicit schema pattern has been defined, only explicit schema patterns will be considered
    *       part of this schema.
    * @see {{{org.opencypher.okapi.api.schema.Schema#schemaPatterns}}}
    * @param patterns the patterns to add
    * @return schema with added explicit schema patterns
    */
  @experimental
  def withSchemaPatterns(patterns: SchemaPattern*): Schema

  /**
    * Returns the union of the input schemas.
    * Conflicting property key types are resolved into the joined type.
    */
  def ++(other: Schema): Schema

  /**
    * Returns this schema with the properties for `combo` removed.
    *
    * @param labelCombination label combination for which properties are removed
    * @return updated schema
    */
  private[opencypher] def dropPropertiesFor(labelCombination: Set[String]): Schema

  /**
    * Returns the sub-schema for a node scan under the given constraints.
    * Labels are interpreted as constraints on the resulting Schema.
    * If no labels are specified, this means the resulting node can have any valid label combination.
    *
    * @param knownLabels Specifies the labels that the node is guaranteed to have
    * @return sub-schema for `knownLabels`
    */
  private[opencypher] def forNode(knownLabels: Set[String]): Schema

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
  private[opencypher] def withOverwrittenNodePropertyKeys(
    labelCombination: Set[String],
    propertyKeys: PropertyKeys
  ): Schema

  /**
    * Returns the updated schema, but overwrites any existing relationship property keys for the given type.
    */
  private[opencypher] def withOverwrittenRelationshipPropertyKeys(relType: String, propertyKeys: PropertyKeys): Schema

  def toString: String

  def toJson: String

  def pretty: String

  def isEmpty: Boolean
}
