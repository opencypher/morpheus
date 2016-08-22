package org.opencypher.spark.api.frame

import org.apache.spark.sql.types._
import org.opencypher.spark.api.CypherType
import org.opencypher.spark.api.types._

object Representation {

  def forCypherType(typ: CypherType): Representation = typ.material match {
    case CTInteger => EmbeddedRepresentation(LongType)
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
  def isEmbedded: Boolean
}

case object BinaryRepresentation extends Representation {
  def dataType = BinaryType
  def isEmbedded = false
}

final case class EmbeddedRepresentation(dataType: DataType) extends Representation {
  def isEmbedded = true
}

