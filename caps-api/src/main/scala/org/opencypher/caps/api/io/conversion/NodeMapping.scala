package org.opencypher.caps.api.io.conversion

object NodeMapping {
  val empty = MissingSourceIdKey

  object MissingSourceIdKey {
    /**
      *
      * @param sourceIdKey
      * @return
      */
    def withSourceIdKey(sourceIdKey: String) = NodeMapping(sourceIdKey)
  }

}

/**
  * Represents a mapping from a source with key-based access to node components (e.g. a table definition) to a Cypher
  * node. The purpose of this class is to define a mapping from an external data source to a property graph.
  *
  * The [[sourceIdKey]] represents a key to the node identifier within the source data. The retrieved value from the
  * source data is expected to be a [[Long]] value that is unique among nodes.
  *
  * The [[labelMapping]] represents a map from node labels to keys in the source data. The retrieved value from the
  * source data is expected to be a [[Boolean]] value indicating if the label is present on that node. If the label is
  * implied, e.g., is present on all nodes in the source data, then the value for that label is [[None]].
  *
  * The [[propertyMapping]] represents a map from node property keys to keys in the source data. The retrieved value
  * from the source is expected to be convertible to a valid [[org.opencypher.caps.api.value.CypherValue]].
  *
  * @param sourceIdKey     key to access the node identifier in the source data
  * @param labelMapping    mapping from label to source key
  * @param propertyMapping mapping from property key source key
  */
case class NodeMapping(
  sourceIdKey: String,
  labelMapping: Map[String, Option[String]] = Map.empty,
  propertyMapping: Map[String, String] = Map.empty) {

  def withImpliedLabel(label: String): NodeMapping =
    copy(labelMapping = labelMapping.updated(label, None))

  def withImpliedLabels(labels: String*): NodeMapping =
    labels.foldLeft(this)((mapping, label) => mapping.withImpliedLabel(label))

  def withOptionalLabel(label: String): NodeMapping =
    withOptionalLabel(label, label)

  def withOptionalLabels(labels: String*): NodeMapping =
    labels.foldLeft(this)((mapping, label) => mapping.withOptionalLabel(label, label))

  def withOptionalLabel(sourceLabelKey: String, label: String): NodeMapping =
    copy(labelMapping = labelMapping.updated(label, Some(sourceLabelKey)))

  def withOptionalLabel(tuple: (String, String)): RelationshipMapping =
    withOptionalLabel(tuple._1 -> tuple._2)

  def withPropertyKey(sourcePropertyKey: String, propertyKey: String): NodeMapping =
    copy(propertyMapping = propertyMapping.updated(propertyKey, sourcePropertyKey))

  def withPropertyKey(tuple: (String, String)): NodeMapping =
    withPropertyKey(tuple._1, tuple._2)

  def withPropertyKey(property: String): NodeMapping =
    withPropertyKey(property, property)

  def withPropertyKeys(properties: String*): NodeMapping =
    properties.foldLeft(this)((mapping, propertyKey) => mapping.withPropertyKey(propertyKey, propertyKey))

}
