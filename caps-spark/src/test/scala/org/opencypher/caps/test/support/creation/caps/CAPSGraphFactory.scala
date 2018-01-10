package org.opencypher.caps.test.support.creation.caps

import org.apache.spark.sql.types.StructField
import org.opencypher.caps.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.spark.CAPSGraph
import org.opencypher.caps.impl.convert.fromJavaType
import org.opencypher.caps.impl.spark.convert.toSparkType
import org.opencypher.caps.test.support.creation.propertygraph.{Node, PropertyGraph, Relationship}

trait CAPSGraphFactory {
  def propertyGraph: PropertyGraph

  def graph: CAPSGraph

  lazy val schema: Schema = {
    def extractFromNode(n: Node) =
      n.labels -> n.properties.map {
        case (name, prop) => name -> fromJavaType(prop)
      }

    def extractFromRel(r: Relationship) =
      r.relType -> r.properties.map {
        case (name, prop) => name -> fromJavaType(prop)
      }

    val labelsAndProps = propertyGraph.nodes.map(extractFromNode)
    val typesAndProps = propertyGraph.relationships.map(extractFromRel)

    val schemaWithLabels = labelsAndProps.foldLeft(Schema.empty) {
      case (acc, (labels, props)) => acc.withNodePropertyKeys(labels, props)
    }

    typesAndProps.foldLeft(schemaWithLabels) {
      case (acc, (t, props)) => acc.withRelationshipPropertyKeys(t)(props.toSeq: _*)
    }
  }

  protected def getPropertyStructFields(propKeys: PropertyKeys): Seq[StructField] = {
    propKeys.foldLeft(Seq.empty[StructField]) {
      case (fields, key) => fields :+ StructField(key._1, toSparkType(key._2), key._2.isNullable)
    }
  }
}
