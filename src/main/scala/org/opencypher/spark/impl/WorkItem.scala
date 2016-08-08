package org.opencypher.spark.impl

import org.opencypher.spark.{CypherString, CypherNode}

case class WorkItem(fields: Seq[StdField]) extends (Product => Product) with Serializable {
  def apply(p: Product): Product = {
    val node = p.productIterator.toVector(0).asInstanceOf[CypherNode]
    val results = fields.map { field =>
      node.properties.get(field.name.name).orNull match {
        case CypherString(s) => s
        case v => v
      }
    }
    productize(results)
  }
}
