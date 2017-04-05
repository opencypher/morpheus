package org.opencypher.spark_legacy.impl

import org.apache.spark.sql.Dataset
import org.opencypher.spark.api.value.{CypherNode, CypherRelationship}
import org.opencypher.spark_legacy.impl.util.SlotSymbolGenerator

class PlanningContext(val slotNames: SlotSymbolGenerator,
                      val nodes: Dataset[CypherNode],
                      val relationships: Dataset[CypherRelationship]) {

  def newSlotSymbol(field: StdField): Symbol =
    slotNames.newSlotSymbol(field)
}








