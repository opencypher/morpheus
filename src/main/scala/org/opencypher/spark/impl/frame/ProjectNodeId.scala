package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.api.CypherNode
import org.opencypher.spark.impl._

object ProjectNodeId {

  def apply(input: StdCypherFrame[Product], nodeField: StdField)(outputField: StdField)(implicit context: PlanningContext): ProjectFrame = {
    new ProjectNodeId(input, nodeField, outputField)(input.signature.addIntegerField(outputField))
  }

  private final class ProjectNodeId(input: StdCypherFrame[Product], nodeField: StdField, outputField: StdField)(sig: StdFrameSignature) extends ProjectFrame(outputField, sig) {

    val index = sig.slot(nodeField).getOrElse(throw new IllegalArgumentException("Unknown nodeField")).ordinal

    override def execute(implicit context: StdRuntimeContext): Dataset[Product] = {
      val in = input.run
      val out = in.map(ProductNodeId(index))(context.productEncoder(slots))
      out
    }
  }

  final case class ProductNodeId(index: Int) extends (Product => Product) {

    import org.opencypher.spark.impl.util._

    def apply(product: Product): Product = {
      val node = product.getAs[CypherNode](index)
      val result = product :+ node.id.v
      result
    }
  }

}
