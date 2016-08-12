package org.opencypher.spark.impl

import org.apache.spark.sql.Dataset
import org.opencypher.spark.api.{CypherNode, CypherRelationship}
import org.opencypher.spark.impl.frame.{AllNodes, AllRelationships}
import org.opencypher.spark.impl.util.SlotSymbolGenerator

class PlanningContext(val slotNames: SlotSymbolGenerator,
                      val nodes: Dataset[CypherNode],
                      val relationships: Dataset[CypherRelationship]) {

  def newSlotSymbol(field: StdField): Symbol =
    slotNames.newSlotSymbol(field)
}








