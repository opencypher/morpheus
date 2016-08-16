package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.api.{CypherNode, CypherRelationship, CypherType}
import org.opencypher.spark.api.types.{CTInteger, CTNode, CTRelationship}
import org.opencypher.spark.impl._

object ProjectFromEntity {

  import FrameVerification._

  def relationshipStartId(input: StdCypherFrame[Product])(relationship: Symbol)(output: Symbol)
                         (implicit context: PlanningContext): ProjectFrame = {
    val (outputField, sig) = input.signature.addField(output -> CTInteger)
    val relField = input.signature.field(relationship)
    requireType(relField, CTRelationship)
    new ProjectFromEntity[CypherRelationship](input)(relField, relationshipStartId, outputField)(sig)
  }

  def relationshipEndId(input: StdCypherFrame[Product])(relationship: Symbol)(output: Symbol)
                       (implicit context: PlanningContext): ProjectFrame = {
    val (outputField, sig) = input.signature.addField(output -> CTInteger)
    val relField = input.signature.field(relationship)
    requireType(relField, CTRelationship)
    new ProjectFromEntity[CypherRelationship](input)(relField, relationshipEndId, outputField)(sig)
  }

  def relationshipId(input: StdCypherFrame[Product])(relationship: Symbol)(output: Symbol)
                    (implicit context: PlanningContext): ProjectFrame = {
    val (outputField, signature) = input.signature.addField(output -> CTInteger)
    val relField = input.signature.field(relationship)
    requireType(relField, CTRelationship)
    new ProjectFromEntity[CypherRelationship](input)(relField, relationshipId, outputField)(signature)
  }

  def nodeId(input: StdCypherFrame[Product])(node: Symbol)(output: Symbol)
            (implicit context: PlanningContext): ProjectFrame = {
    val (outputField, signature) = input.signature.addField(output -> CTInteger)
    val nodeField = input.signature.field(node)
    requireType(nodeField, CTNode)
    new ProjectFromEntity[CypherNode](input)(nodeField, nodeId, outputField)(signature)
  }

  private def requireType(field: StdField, typ: CypherType) = {
    verify(field.cypherType == typ) failWith {
      CypherTypeError(s"Expected $typ, but got: ${field.cypherType}")
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

  private final case class nodeId(override val index: Int)
    extends ProjectEntityPrimitive[CypherNode] {
    override def extract(node: CypherNode) = node.id.v
  }

  private final case class relationshipId(override val index: Int)
    extends ProjectEntityPrimitive[CypherRelationship] {
    override def extract(relationship: CypherRelationship) = relationship.id.v
  }

  protected[frame] final case class CypherTypeError(msg: String) extends FrameVerificationError(msg)
}
