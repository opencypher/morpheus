package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.api.{CypherNode, CypherRelationship}
import org.opencypher.spark.impl._

object ProjectRelationshipEndId {

  def apply(input: StdCypherFrame[Product], relField: StdField)(outputField: StdField)(implicit context: PlanningContext): ProjectFrame =
    new ProjectRelationshipEndId(input, relField, outputField)(input.signature.addIntegerField(outputField))

  private final class ProjectRelationshipEndId(input: StdCypherFrame[Product], relField: StdField, outputField: StdField)(sig: StdFrameSignature) extends ProjectFrame(outputField, sig) {

    val index = sig.slot(relField).ordinal

    override def execute(implicit context: StdRuntimeContext): Dataset[Product] = {
      val in = input.run
      val out = in.map(RelationshipEndId(index))(context.productEncoder(slots))
      out
    }
  }

  private final case class RelationshipEndId(index: Int) extends (Product => Product) {

    import org.opencypher.spark.impl.util._

    def apply(product: Product): Product = {
      val relationship = product.getAs[CypherRelationship](index)
      val result = product :+ relationship.endId.v
      result
    }
  }

}
