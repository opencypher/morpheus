package org.opencypher.spark.impl.convert

import org.apache.spark.sql.types._
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.exception.NotImplementedException

object CAPSCypherType {
  implicit class RichCypherType(val ct: CypherType) extends AnyVal {
    def toSparkType: Option[DataType] = ct match {
      case CTNull | CTVoid => Some(NullType)
      case _ =>
        ct.material match {
          case CTString => Some(StringType)
          case CTInteger => Some(LongType)
          case CTBoolean => Some(BooleanType)
          case CTFloat => Some(DoubleType)
          case _: CTNode => Some(LongType)
          case _: CTRelationship => Some(LongType)
          case CTList(elemType) =>
            elemType.toSparkType.map(ArrayType(_, elemType.isNullable))
          case _ =>
            None
        }
    }

    def getSparkType: DataType = toSparkType match {
      case Some(t) => t
      case None => throw NotImplementedException(s"Mapping of CypherType $ct to Spark type")
    }

    def isSparkCompatible: Boolean = toSparkType.isDefined
  }

  // Spark data types that are supported within the Cypher type system
  val supportedTypes = Seq(
    // numeric
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    // other
    StringType,
    BooleanType,
    NullType
  )

  implicit class RichDataType(val dt: DataType) extends AnyVal {
    def toCypherType(nullable: Boolean = false): Option[CypherType] = {
      val result = dt match {
        case StringType => Some(CTString)
        case LongType => Some(CTInteger)
        case BooleanType => Some(CTBoolean)
        case BinaryType => Some(CTAny)
        case DoubleType => Some(CTFloat)
        case ArrayType(elemType, containsNull) =>
          elemType.toCypherType(containsNull).map(CTList)
        case NullType => Some(CTNull)
        case _ => None
      }

      if (nullable) result.map(_.nullable) else result.map(_.material)
    }

    /**
      * Checks if the given data type is supported within the Cypher type system.
      *
      * @return true, iff the data type is supported
      */
    def isCypherCompatible: Boolean = dt match {
      case ArrayType(internalType, _) => internalType.isCypherCompatible
      case other => supportedTypes.contains(other)
    }

    /**
      * Converts the given Spark data type into a Cypher type system compatible Spark data type.
      *
      * @return some Cypher-compatible Spark data type or none if not compatible
      */
    def cypherCompatibleDataType: Option[DataType] = dt match {
      case ByteType | ShortType | IntegerType => Some(LongType)
      case FloatType => Some(DoubleType)
      case compatible if dt.toCypherType().isDefined => Some(compatible)
      case _ => None
    }
  }
}
