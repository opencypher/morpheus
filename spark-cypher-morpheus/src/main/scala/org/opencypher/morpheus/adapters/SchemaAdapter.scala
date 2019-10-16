package org.opencypher.morpheus.adapters

import org.apache.spark.graph.api.PropertyGraphSchema
import org.opencypher.okapi.api.schema.{PropertyGraphSchema => OKAPISchema}

case class SchemaAdapter(schema: OKAPISchema) extends PropertyGraphSchema {

  override def labelSets: Array[Array[String]] = schema.labelCombinations.combos.map(_.toArray).toArray

  override def relationshipTypes: Array[String] = schema.relationshipTypes.toArray


}
