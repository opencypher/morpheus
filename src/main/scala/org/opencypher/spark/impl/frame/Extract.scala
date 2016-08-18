package org.opencypher.spark.impl.frame

import java.{lang => Java}

import org.apache.spark.sql.Dataset
import org.opencypher.spark.api._
import org.opencypher.spark.api.types._
import org.opencypher.spark.impl._

import scala.language.postfixOps

object Extract extends FrameCompanion {

  def relationshipStartId(input: StdCypherFrame[Product])(relationship: Symbol)(output: Symbol)
                         (implicit context: PlanningContext): ProjectFrame = {
    val relField = obtain(input.signature.field)(relationship)
    requireMateriallyIsSubTypeOf(relField.cypherType, CTRelationship)
    val (outputField, sig) = input.signature.addField(output -> CTInteger.asNullableAs(relField.cypherType))
    new Extract[CypherRelationship](input)(relField, relationshipStartId, outputField)(sig)
  }

  def relationshipEndId(input: StdCypherFrame[Product])(relationship: Symbol)(output: Symbol)
                       (implicit context: PlanningContext): ProjectFrame = {
    val relField = obtain(input.signature.field)(relationship)
    requireMateriallyIsSubTypeOf(relField.cypherType, CTRelationship)
    val (outputField, sig) = input.signature.addField(output -> CTInteger.asNullableAs(relField.cypherType))
    new Extract[CypherRelationship](input)(relField, relationshipEndId, outputField)(sig)
  }

  def relationshipId(input: StdCypherFrame[Product])(relationship: Symbol)(output: Symbol)
                    (implicit context: PlanningContext): ProjectFrame = {
    val relField = obtain(input.signature.field)(relationship)
    requireMateriallyIsSubTypeOf(relField.cypherType, CTRelationship)
    val (outputField, signature) = input.signature.addField(output -> CTInteger.asNullableAs(relField.cypherType))
    new Extract[CypherRelationship](input)(relField, relationshipId, outputField)(signature)
  }

  def relationshipType(input: StdCypherFrame[Product])(relationship: Symbol)(output: Symbol)
                      (implicit context: PlanningContext): ProjectFrame = {
    val relField = obtain(input.signature.field)(relationship)
    requireMateriallyIsSubTypeOf(relField.cypherType, CTRelationship)
    val (outputField, signature) = input.signature.addField(output -> CTString.asNullableAs(relField.cypherType))
    new Extract[CypherRelationship](input)(relField, relationshipType, outputField)(signature)
  }

  def nodeId(input: StdCypherFrame[Product])(node: Symbol)(output: Symbol)
            (implicit context: PlanningContext): ProjectFrame = {
    val nodeField = obtain(input.signature.field)(node)
    requireMateriallyIsSubTypeOf(nodeField.cypherType, CTNode)
    val (outputField, signature) = input.signature.addField(output -> CTInteger.asNullableAs(nodeField.cypherType))
    new Extract[CypherNode](input)(nodeField, nodeId, outputField)(signature)
  }

  def property(input: StdCypherFrame[Product])(map: Symbol,propertyKey: Symbol)(output: Symbol)
              (implicit context: PlanningContext): ProjectFrame = {

    val mapField = obtain(input.signature.field)(map)
    requireMateriallyIsSubTypeOf(mapField.cypherType, CTMap)
    val (outputField, signature) = input.signature.addField(output -> CTAny.nullable)
    new Extract[CypherAnyMap](input)(mapField, propertyValue(propertyKey), outputField)(signature)
  }

  private final case class Extract[T](input: StdCypherFrame[Product])(entityField: StdField,
                                                                      primitive: Int => ExtractPrimitive[T],
                                                                      outputField: StdField)
                                     (sig: StdFrameSignature) extends ProjectFrame(outputField, sig) {

    val index = obtain(sig.fieldSlot)(entityField).ordinal

    override def execute(implicit context: StdRuntimeContext): Dataset[Product] = {
      val in = input.run
      val out = in.map(primitive(index))(context.productEncoder(slots))
      out
    }
  }

  sealed trait ExtractPrimitive[T] extends (Product => Product) {
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
    extends ExtractPrimitive[CypherRelationship] {
      override def extract(relationship: CypherRelationship): Java.Long = relationship.startId.v
  }

  private final case class relationshipEndId(override val index: Int)
    extends ExtractPrimitive[CypherRelationship] {
    override def extract(relationship: CypherRelationship): Java.Long = relationship.endId.v
  }

  private final case class nodeId(override val index: Int)
    extends ExtractPrimitive[CypherNode] {
    override def extract(node: CypherNode): Java.Long  = node.id.v
  }

  private final case class relationshipId(override val index: Int)
    extends ExtractPrimitive[CypherRelationship] {
    override def extract(relationship: CypherRelationship): Java.Long = relationship.id.v
  }

  private final case class relationshipType(override val index: Int)
    extends ExtractPrimitive[CypherRelationship] {
    override def extract(relationship: CypherRelationship) = relationship.relationshipType
  }

  private final case class propertyValue(propertyKey: Symbol)(override val index: Int)
    extends ExtractPrimitive[CypherAnyMap] {
    override def extract(v: CypherAnyMap) =
      // TODO: Make nice
      v.properties.getOrElse(propertyKey.name, null).asInstanceOf[AnyRef]
  }
}
