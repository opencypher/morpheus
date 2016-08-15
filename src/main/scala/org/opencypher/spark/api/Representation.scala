package org.opencypher.spark.api

import org.apache.spark.sql.types._
import org.opencypher.spark.api.types._

object Representation {

  def sparkReprForCypherType(typ: CypherType): Representation = typ.material match {
    case CTInteger => EmbeddedRepresentation(IntegerType)
    case CTFloat => EmbeddedRepresentation(DoubleType)
    case CTBoolean => EmbeddedRepresentation(BooleanType)
    case CTString => EmbeddedRepresentation(StringType)
    case CTVoid => EmbeddedRepresentation(NullType)
    case _: CTList => BinaryRepresentation
    case CTNode | CTRelationship | CTPath | CTMap | CTAny | CTNumber | CTWildcard => BinaryRepresentation
  }
}

sealed trait Representation extends Serializable {
  def dataType: DataType
}

case object BinaryRepresentation extends Representation {
  def dataType = BinaryType
}

final case class EmbeddedRepresentation(dataType: DataType) extends Representation

