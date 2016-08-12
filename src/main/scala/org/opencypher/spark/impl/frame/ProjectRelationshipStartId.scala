package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.CypherRelationship
import org.opencypher.spark.impl._
import org.opencypher.spark.impl.util.productize

object ProjectRelationshipStartId {

  def apply(input: StdCypherFrame[Product], relField: StdField)(outputField: StdField)(implicit context: PlanningContext): ProjectFrame =
    new ProjectRelationshipStartId(input, relField, outputField)(input.signature.addIntegerField(outputField))

  private final class ProjectRelationshipStartId(input: StdCypherFrame[Product], relField: StdField, outputField: StdField)(sig: StdFrameSignature) extends ProjectFrame(outputField, sig) {

    val index = sig(relField).getOrElse(throw new IllegalArgumentException("Unknown relationship field")).ordinal

    override def run(implicit context: StdRuntimeContext): Dataset[Product] = {
      val in = input.run
      val mapped = in.map(RelationshipStartId(index))(context.productEncoder(slots))
      alias(mapped, context.productEncoder(slots))
    }
  }

  private final case class RelationshipStartId(index: Int) extends (Product => Product) {
    def apply(product: Product): Product = {
      val elts = product.productIterator.toVector
      val relationship = elts(index).asInstanceOf[CypherRelationship]
      val result = productize(elts :+ relationship.start.v)
      result
    }
  }

}
