package org.opencypher.spark.impl.frame

import java.{lang => Java}

import org.apache.spark.sql.Dataset
import org.opencypher.spark.api._
import org.opencypher.spark.api.types._
import org.opencypher.spark.impl._

object Extract {

  import FrameVerification._

  def relationshipStartId(input: StdCypherFrame[Product])(relationship: Symbol)(output: Symbol)
                         (implicit context: PlanningContext): ProjectFrame = {
    val relField = input.signature.field(relationship)
    requireType(relField, CTRelationship.nullable)
    val (outputField, sig) = input.signature.addField(output -> CTInteger.asNullableAs(relField.cypherType))
    new Extract[CypherRelationship](input)(relField, relationshipStartId, outputField)(sig)
  }

  def relationshipEndId(input: StdCypherFrame[Product])(relationship: Symbol)(output: Symbol)
                       (implicit context: PlanningContext): ProjectFrame = {
    val relField = input.signature.field(relationship)
    requireType(relField, CTRelationship.nullable)
    val (outputField, sig) = input.signature.addField(output -> CTInteger.asNullableAs(relField.cypherType))
    new Extract[CypherRelationship](input)(relField, relationshipEndId, outputField)(sig)
  }

  def relationshipId(input: StdCypherFrame[Product])(relationship: Symbol)(output: Symbol)
                    (implicit context: PlanningContext): ProjectFrame = {
    val relField = input.signature.field(relationship)
    requireType(relField, CTRelationship.nullable)
    val (outputField, signature) = input.signature.addField(output -> CTInteger.asNullableAs(relField.cypherType))
    new Extract[CypherRelationship](input)(relField, relationshipId, outputField)(signature)
  }

  def nodeId(input: StdCypherFrame[Product])(node: Symbol)(output: Symbol)
            (implicit context: PlanningContext): ProjectFrame = {
    val nodeField = input.signature.field(node)
    requireType(nodeField, CTNode.nullable)
    val (outputField, signature) = input.signature.addField(output -> CTInteger.asNullableAs(nodeField.cypherType))
    new Extract[CypherNode](input)(nodeField, nodeId, outputField)(signature)
  }

  def property(input: StdCypherFrame[Product])(map: Symbol,propertyKey: Symbol)(output: Symbol)
              (implicit context: PlanningContext): ProjectFrame = {

    val mapField = input.signature.field(map)
    requireType(mapField, CTMap.nullable)
    val (outputField, signature) = input.signature.addField(output -> CTAny.nullable)
    new Extract[CypherAnyMap](input)(mapField, propertyValue(propertyKey), outputField)(signature)
  }

  private def requireType(field: StdField, typ: CypherType) = {
    verify(field.cypherType `subTypeOf` typ isTrue) failWith {
      CypherTypeError(s"Expected $typ, but got: ${field.cypherType}")
    }
  }

  private final class Extract[T](input: StdCypherFrame[Product])(entityField: StdField,
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
    def extract(value: T): AnyRef

    import org.opencypher.spark.impl.util._

    def apply(product: Product): Product = {
      val entity = product.getAs[T](index)
      val value = if (entity == null) null else extract(entity)
      val result = product :+ value
      result
    }
  }

  private final case class relationshipStartId(override val index: Int)
    extends ProjectEntityPrimitive[CypherRelationship] {
      override def extract(relationship: CypherRelationship): Java.Long = relationship.startId.v
  }

  private final case class relationshipEndId(override val index: Int)
    extends ProjectEntityPrimitive[CypherRelationship] {
    override def extract(relationship: CypherRelationship): Java.Long = relationship.endId.v
  }

  private final case class nodeId(override val index: Int)
    extends ProjectEntityPrimitive[CypherNode] {
    override def extract(node: CypherNode): Java.Long  = node.id.v
  }

  private final case class relationshipId(override val index: Int)
    extends ProjectEntityPrimitive[CypherRelationship] {
    override def extract(relationship: CypherRelationship): Java.Long = relationship.id.v
  }

  private final case class propertyValue(propertyKey: Symbol)(override val index: Int)
    extends ProjectEntityPrimitive[CypherAnyMap] {
    override def extract(v: CypherAnyMap) =
      // TODO: Make nice
      v.properties.getOrElse(propertyKey.name, null).asInstanceOf[AnyRef]
  }

  protected[frame] final case class CypherTypeError(msg: String) extends FrameVerificationError(msg)
}
