package org.opencypher.spark

import org.apache.spark.sql.{Column, functions}

object IDEncoding {

  type CAPSId = Array[Byte]

  def encode(l: Long): CAPSId = {
    val a = new Array[Byte](8)
    a(0) = (l >> 56).toByte
    a(1) = (l >> 48).toByte
    a(2) = (l >> 40).toByte
    a(3) = (l >> 32).toByte
    a(4) = (l >> 24).toByte
    a(5) = (l >> 16).toByte
    a(6) = (l >> 8).toByte
    a(7) = l.toByte
    a
  }

  private val encodeUdf = functions.udf(encode _)

  def addPrefix(a: CAPSId, p: Byte): CAPSId = {
    val n = new Array[Byte](a.length + 1)
    n(0) = p
    System.arraycopy(a, 0, n, 1, a.length)
    n
  }

  private val addPrefixUdf = functions.udf(addPrefix _)

  implicit class ColumnIdEncoding(val c: Column) extends AnyVal {

    def addPrefix(p: Byte): Column = addPrefixUdf(c, functions.lit(p))

    def encodeLongAsCAPSId(name: String): Column = encodeLongAsCAPSId.as(name)

    def encodeLongAsCAPSId: Column = encodeUdf(c)

    //    def encodeLongAsCAPSId: Column = {
    //      lit(array(
    //        shiftRightUnsigned(c, 56).cast(ByteType),
    //        shiftRightUnsigned(c, 48).cast(ByteType),
    //        shiftRightUnsigned(c, 40).cast(ByteType),
    //        shiftRightUnsigned(c, 32).cast(ByteType),
    //        shiftRightUnsigned(c, 24).cast(ByteType),
    //        shiftRightUnsigned(c, 16).cast(ByteType),
    //        shiftRightUnsigned(c, 8).cast(ByteType),
    //        c.cast(ByteType)
    //      )).cast(ArrayType(ByteType))
    //    }

  }

  implicit class LongIdEncoding(val l: Long) extends AnyVal {

    def encodeAsCAPSId: CAPSId = encode(l)

  }

//  implicit class RichCAPSId(val id: CAPSId) extends AnyVal {
//
//    def withPrefix(prefix: GraphIdPrefix): CAPSId = addPrefix(id, prefix)
//
//  }

}