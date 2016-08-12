package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.api.CypherRelationship
import org.opencypher.spark.impl._

object ProjectRelationshipEndId {

  def apply(input: StdCypherFrame[Product], relField: StdField)(outputField: StdField)(implicit context: PlanningContext): ProjectFrame =
    new ProjectRelationshipEndId(input, relField, outputField)(input.signature.addIntegerField(outputField))

  private final class ProjectRelationshipEndId(input: StdCypherFrame[Product], relField: StdField, outputField: StdField)(sig: StdFrameSignature) extends ProjectFrame(sig) {

    val index = sig.slot(relField).getOrElse(throw new IllegalArgumentException("Unknown relationship field")).ordinal

    override def run(implicit context: StdRuntimeContext): Dataset[Product] = {
      val in = input.run
      val mapped = in.map(RelationshipEndId(index))(context.productEncoder(slots))
      alias(mapped)(context.productEncoder(slots))
    }

    override def projectedField = outputField
  }

  private final case class RelationshipEndId(index: Int) extends (Product => Product) {

    import org.opencypher.spark.impl.util._

    def apply(product: Product): Product = {
      val elts = product.toVector
      val relationship = elts(index).asInstanceOf[CypherRelationship]
      val result = (elts :+ relationship.end.v).toProduct
      result
    }
  }

}
