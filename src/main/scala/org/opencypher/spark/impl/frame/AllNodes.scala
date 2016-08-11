package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.CypherTypes.CTNode
import org.opencypher.spark.impl._
import org.opencypher.spark.{CypherNode, CypherValue}

object AllNodes {

  def apply(input: Dataset[CypherNode])(fieldSym: Symbol)(implicit context: PlanningContext): CypherNodes = {
    val field = StdField(fieldSym, CTNode)
    new CypherNodes(
      input = input,
      sig = StdFrameSignature.empty.addField(field)
    )
  }

  class CypherNodes(input: Dataset[CypherNode], sig: StdFrameSignature)
    extends StdCypherFrame[CypherNode](sig) {

    override def run(implicit context: RuntimeContext): Dataset[CypherNode] = {
      // rename hard-coded column name 'value' to our slot name
      alias(input, CypherValue.implicits.cypherValueEncoder[CypherNode])
    }

    def nodeField: StdField = sig.fields.head
  }
}
