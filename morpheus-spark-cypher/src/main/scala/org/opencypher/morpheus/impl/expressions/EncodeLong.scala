/**
  * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  *
  * Attribution Notice under the terms of the Apache License 2.0
  *
  * This work was created by the collective efforts of the openCypher community.
  * Without limiting the terms of Section 6, any Derivative Work that is not
  * approved by the public consensus process of the openCypher Implementers Group
  * should not be described as “Cypher” (and Cypher® is a registered trademark of
  * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
  * proposals for change that have been documented or implemented should only be
  * described as "implementation extensions to Cypher" or as "proposed changes to
  * Cypher that are not yet approved by the openCypher community".
  */
package org.opencypher.morpheus.impl.expressions

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.types.{BinaryType, DataType, LongType}
import org.opencypher.morpheus.api.value.MorpheusElement._

/**
  * Spark expression that encodes a long into a byte array using variable-length encoding.
  *
  * @param child expression of type LongType that is encoded by this expression
  */
case class EncodeLong(child: Expression) extends UnaryExpression with NullIntolerant with ExpectsInputTypes {

  override val dataType: DataType = BinaryType

  override val inputTypes: Seq[LongType] = Seq(LongType)

  override protected def nullSafeEval(input: Any): Any =
    EncodeLong.encodeLong(input.asInstanceOf[Long])

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, c => s"(byte[])(${EncodeLong.getClass.getName.dropRight(1)}.encodeLong($c))")
}

object EncodeLong {

  private final val moreBytesBitMask: Long = Integer.parseInt("10000000", 2)
  private final val varLength7BitMask: Long = Integer.parseInt("01111111", 2)
  private final val otherBitsMask = ~varLength7BitMask
  private final val maxBytesForLongVarEncoding = 10

  // Same encoding as as Base 128 Varints @ https://developers.google.com/protocol-buffers/docs/encoding
  @inline
  final def encodeLong(l: Long): Array[Byte] = {
    val tempResult = new Array[Byte](maxBytesForLongVarEncoding)

    var remainder = l
    var index = 0

    while ((remainder & otherBitsMask) != 0) {
      tempResult(index) = ((remainder & varLength7BitMask) | moreBytesBitMask).toByte
      remainder >>>= 7
      index += 1
    }
    tempResult(index) = remainder.toByte

    val result = new Array[Byte](index + 1)
    System.arraycopy(tempResult, 0, result, 0, index + 1)
    result
  }

  // Same encoding as as Base 128 Varints @ https://developers.google.com/protocol-buffers/docs/encoding
  @inline
  final def decodeLong(input: Array[Byte]): Long = {
    assert(input.nonEmpty, "`decodeLong` requires a non-empty array as its input")
    var index = 0
    var currentByte = input(index)
    var decoded = currentByte & varLength7BitMask
    var nextLeftShift = 7

    while ((currentByte & moreBytesBitMask) != 0) {
      index += 1
      currentByte = input(index)
      decoded |= (currentByte & varLength7BitMask) << nextLeftShift
      nextLeftShift += 7
    }
    assert(index == input.length - 1,
      s"`decodeLong` received an input array ${input.toSeq.toHex} with extra bytes that could not be decoded.")
    decoded
  }

  implicit class ColumnLongOps(val c: Column) extends AnyVal {

    def encodeLongAsMorpheusId(name: String): Column = encodeLongAsMorpheusId.as(name)

    def encodeLongAsMorpheusId: Column = new Column(EncodeLong(c.expr))

  }

}
