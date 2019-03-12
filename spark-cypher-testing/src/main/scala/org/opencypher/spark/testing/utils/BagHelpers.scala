package org.opencypher.spark.testing.utils

import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.testing.Bag.Bag
import org.opencypher.spark.api.value.{CAPSNode, CAPSRelationship}

object BagHelpers {

  implicit class BagOps(val bag: Bag[CypherMap]) {
    def nodeValuesWithoutIds: Bag[(Set[String], CypherMap)] = for {
      (map, count) <- bag
      value <- map.value.values
      node = value.cast[CAPSNode]
    } yield (node.labels, node.properties) -> count

    def relValuesWithoutIds: Bag[(String, CypherMap)] = for {
      (map, count) <- bag
      value <- map.value.values
      rel = value.cast[CAPSRelationship]
    } yield (rel.relType, rel.properties) -> count
  }
}
