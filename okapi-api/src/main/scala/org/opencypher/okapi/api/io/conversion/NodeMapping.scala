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
package org.opencypher.okapi.api.io.conversion

import org.opencypher.okapi.api.types.CTNode
import org.opencypher.okapi.impl.exception.IllegalArgumentException

object NodeMapping {
  /**
    * Alias for [[withSourceIdKey()]].
    *
    * @param sourceIdKey key to access the node identifier in the source data
    * @return node mapping
    */
  def on(sourceIdKey: String): NodeMapping =
    withSourceIdKey(sourceIdKey)

  /**
    *
    * @param sourceIdKey represents a key to the node identifier within the source data. The retrieved value
    *                    from the source data is expected to be a [[Long]] value that is unique among nodes.
    * @return node mapping
    */
  def withSourceIdKey(sourceIdKey: String): NodeMapping =
    NodeMapping(sourceIdKey)

  /**
    * Creates a NodeMapping where optional labels and property keys match with their corresponding keys in the source
    * data.
    *
    * See [[NodeMapping]] for further information.
    *
    * @param nodeIdKey      key to access the node identifier in the source data
    * @param impliedLabels  set of node labels
    * @param optionalLabels set of optional node labels
    * @param propertyKeys   set of property keys
    * @return node mapping
    */
  def create(nodeIdKey: String, impliedLabels: Set[String] = Set.empty, optionalLabels: Set[String] = Set.empty, propertyKeys: Set[String] = Set.empty): NodeMapping = {

    val mappingWithImpliedLabels = impliedLabels.foldLeft(NodeMapping.withSourceIdKey(nodeIdKey)) {
      (mapping, label) => mapping.withImpliedLabel(label)
    }

    val mappingWithOptionalLabels = optionalLabels.foldLeft(mappingWithImpliedLabels) {
      (mapping, label) => mapping.withOptionalLabel(label)
    }

    propertyKeys.foldLeft(mappingWithOptionalLabels) {
      (mapping, property) => mapping.withPropertyKey(property)
    }
  }
}

/**
  * Represents a mapping from a source with key-based access to node components (e.g. a table definition) to a Cypher
  * node. The purpose of this class is to define a mapping from an external data source to a property graph.
  *
  * Construct a [[NodeMapping]] starting with {{NodeMapping.on}}.
  *
  * The [[sourceIdKey]] represents a key to the node identifier within the source data. The retrieved value from the
  * source data is expected to be a [[Long]] value that is unique among nodes.
  *
  * The [[impliedLabels]] represent a set of node labels.
  *
  * The [[optionalLabelMapping]] represent a map from node labels to keys in the source data. The retrieved value from
  * the source data is expected to be a [[Boolean]] value indicating if the label is present on that node.
  *
  * The [[propertyMapping]] represents a map from node property keys to keys in the source data. The retrieved value
  * from the source is expected to be convertible to a valid [[org.opencypher.okapi.api.value.CypherValue]].
  *
  * @param sourceIdKey          key to access the node identifier in the source data
  * @param impliedLabels        set of node labels
  * @param optionalLabelMapping mapping from label to source key
  * @param propertyMapping      mapping from property key to source property key
  */
final case class NodeMapping private[okapi](
  sourceIdKey: String,
  impliedLabels: Set[String] = Set.empty,
  optionalLabelMapping: Map[String, String] = Map.empty,
  propertyMapping: Map[String, String] = Map.empty) extends EntityMapping {

  // on construction
  validate()

  def cypherType: CTNode = CTNode(impliedLabels)

  def withImpliedLabels(labels: String*): NodeMapping =
    labels.foldLeft(this)((mapping, label) => mapping.withImpliedLabel(label))

  def withImpliedLabel(label: String): NodeMapping =
    copy(impliedLabels = impliedLabels + label)

  def withOptionalLabel(label: String): NodeMapping =
    withOptionalLabel(label, label)

  def withOptionalLabels(labels: String*): NodeMapping =
    labels.foldLeft(this)((mapping, label) => mapping.withOptionalLabel(label, label))

  def withOptionalLabel(label: String, sourceLabelKey: String): NodeMapping =
    copy(optionalLabelMapping = optionalLabelMapping.updated(label, sourceLabelKey))

  def withOptionalLabel(tuple: (String, String)): NodeMapping =
    withOptionalLabel(tuple._1, tuple._2)

  def withPropertyKey(tuple: (String, String)): NodeMapping =
    withPropertyKey(tuple._1, tuple._2)

  def withPropertyKey(property: String): NodeMapping =
    withPropertyKey(property, property)

  def withPropertyKey(propertyKey: String, sourcePropertyKey: String): NodeMapping = {
    preventOverwritingProperty(propertyKey)
    copy(propertyMapping = propertyMapping.updated(propertyKey, sourcePropertyKey))
  }

  def withPropertyKeys(properties: String*): NodeMapping =
    properties.foldLeft(this)((mapping, propertyKey) => mapping.withPropertyKey(propertyKey, propertyKey))

  private def validate(): Unit = {
    if (optionalLabelMapping.values.toSet.contains(sourceIdKey))
      throw IllegalArgumentException("source id key and optional labels referring to different source keys",
        s"$sourceIdKey used for source id key and optional label")
  }
}
