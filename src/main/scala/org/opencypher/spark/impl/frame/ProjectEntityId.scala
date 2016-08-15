package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.api.CypherEntity
import org.opencypher.spark.api.types.CTInteger
import org.opencypher.spark.impl._

object ProjectEntityId {

  def apply(input: StdCypherFrame[Product])(entity: Symbol)(output: Symbol)
           (implicit context: PlanningContext): ProjectFrame = {
    val outputField: StdField = StdField(output, CTInteger)
    val signature: StdFrameSignature = input.signature.addIntegerField(outputField)
    new ProjectNodeId(input)(input.signature.field(entity), outputField)(signature)
  }

  private final class ProjectNodeId(input: StdCypherFrame[Product])
                                   (entityField: StdField, outputField: StdField)(sig: StdFrameSignature)
    extends ProjectFrame(outputField, sig) {

    val index = sig.slot(entityField).ordinal

    override def execute(implicit context: StdRuntimeContext): Dataset[Product] = {
      val in = input.run
      val out = in.map(entityId(index))(context.productEncoder(slots))
      out
    }
  }

  private final case class entityId(index: Int) extends (Product => Product) {

    import org.opencypher.spark.impl.util._

    def apply(product: Product): Product = {
      val entity = product.getAs[CypherEntity](index)
      val result = product :+ entity.id.v
      result
    }
  }

}
