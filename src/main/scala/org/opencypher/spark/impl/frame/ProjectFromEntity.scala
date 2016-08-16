package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.api.CypherRelationship
import org.opencypher.spark.api.types.{CTInteger, CTRelationship}
import org.opencypher.spark.impl._

object ProjectFromEntity {

  import FrameVerification._

  def relationshipStartId(input: StdCypherFrame[Product])(relationship: Symbol)(output: Symbol)
                         (implicit context: PlanningContext): ProjectFrame = {
    val (field, sig) = input.signature.addField(output -> CTInteger)
    val entityField = input.signature.field(relationship)
    requireRelationship(entityField)
    new ProjectFromEntity[CypherRelationship](input)(entityField, relationshipStartId, field)(sig)
  }

  def relationshipEndId(input: StdCypherFrame[Product])(relationship: Symbol)(output: Symbol)
                       (implicit context: PlanningContext): ProjectFrame = {
    val (field, sig) = input.signature.addField(output -> CTInteger)
    val entityField = input.signature.field(relationship)
    requireRelationship(entityField)
    new ProjectFromEntity[CypherRelationship](input)(entityField, relationshipEndId, field)(sig)
  }

  private def requireRelationship(field: StdField) = {
    verify(field.cypherType == CTRelationship) failWith {
      CypherTypeError(s"Expected $CTRelationship, but got: ${field.cypherType}")
    }
  }

  private final class ProjectFromEntity[T](input: StdCypherFrame[Product])(entityField: StdField,
                                                                           primitive: Int => ProjectEntityPrimitive[T],
                                                                           outputField: StdField)
                                          (sig: StdFrameSignature) extends ProjectFrame(outputField, sig) {

    val index = sig.slot(entityField).ordinal

    override def execute(implicit context: StdRuntimeContext): Dataset[Product] = {
      val in = input.run
      val out = in.map(primitive(index))(context.productEncoder(slots))
      out
    }
  }

  sealed trait ProjectEntityPrimitive[T] extends (Product => Product) {
    def index: Int
    def extract(value: T): Any

    import org.opencypher.spark.impl.util._

    def apply(product: Product): Product = {
      val entity = product.getAs[T](index)
      val result = product :+ extract(entity)
      result
    }
  }

  private final case class relationshipStartId(override val index: Int)
    extends ProjectEntityPrimitive[CypherRelationship] {
      override def extract(relationship: CypherRelationship) = relationship.startId.v
  }

  private final case class relationshipEndId(override val index: Int)
    extends ProjectEntityPrimitive[CypherRelationship] {
    override def extract(relationship: CypherRelationship) = relationship.endId.v
  }

  protected[frame] final case class CypherTypeError(msg: String) extends FrameVerificationError(msg)
}
