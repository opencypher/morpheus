package org.opencypher.caps.api.io.conversion

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
  * @param propertyMapping mapping from property key source key
  */
case class RelationshipMapping(
  sourceIdKey: String,
  sourceStartNodeKey: String,
  sourceEndNodeKey: String,
  relTypeOrSourceRelTypeKey: Either[String, String],
  propertyMapping: Map[String, String] = Map.empty) {

  def withPropertyKey(sourcePropertyKey: String, propertyKey: String): RelationshipMapping =
    copy(propertyMapping = propertyMapping.updated(propertyKey, sourcePropertyKey))

  def withPropertyKey(tuple: (String, String)): RelationshipMapping =
    withPropertyKey(tuple._1, tuple._2)

  def withPropertyKey(property: String): RelationshipMapping =
    withPropertyKey(property, property)

  def withPropertyKeys(properties: String*): RelationshipMapping =
    properties.foldLeft(this)((mapping, propertyKey) => mapping.withPropertyKey(propertyKey, propertyKey))

}
