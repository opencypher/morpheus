package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.api.CypherNode
import org.opencypher.spark.impl._

object ProjectNodeId {

  def apply(input: StdCypherFrame[Product], nodeField: StdField)(outputField: StdField)(implicit context: PlanningContext): ProjectFrame = {
    new ProjectNodeId(input, nodeField, outputField)(input.signature.addIntegerField(outputField))
  }

  private final class ProjectNodeId(input: StdCypherFrame[Product], nodeField: StdField, outputField: StdField)(sig: StdFrameSignature) extends ProjectFrame(sig) {

    val index = sig.slot(nodeField).getOrElse(throw new IllegalArgumentException("Unknown nodeField")).ordinal

    override def run(implicit context: StdRuntimeContext): Dataset[Product] = {
      val in = input.run
      val out = in.map(ProductNodeId(index))(context.productEncoder(slots))
      out
    }

    override def projectedField: StdField = outputField
  }

  final case class ProductNodeId(index: Int) extends (Product => Product) {

    import org.opencypher.spark.impl.util._

    def apply(product: Product): Product = {
      val elts = product.toVector
      val node = elts(index).asInstanceOf[CypherNode]
      val result = (elts :+ node.id.v).toProduct
      result
    }
  }

}
