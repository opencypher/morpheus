/**
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
package org.opencypher.spark.impl.expressions


import java.io.ByteArrayOutputStream

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.opencypher.spark.impl.expressions.EncodeLong.encodeLong
import org.opencypher.spark.impl.expressions.Serialize._

case class Serialize(children: Seq[Expression]) extends Expression {

  override def dataType: DataType = BinaryType

  override def nullable: Boolean = false

  // TODO: Only write length if more than one column is serialized
  override def eval(input: InternalRow): Any = {
    // TODO: Reuse from a pool instead of allocating a new one for each serialization
    val out = new ByteArrayOutputStream()
    children.foreach { child =>
      child.dataType match {
        case BinaryType => write(child.eval(input).asInstanceOf[Array[Byte]], out)
        case StringType => write(child.eval(input).asInstanceOf[UTF8String], out)
        case IntegerType => write(child.eval(input).asInstanceOf[Int], out)
        case LongType => write(child.eval(input).asInstanceOf[Long], out)
        case NullType => throwNull(s"$child of type ${child.getClass.getSimpleName}")
      }
    }
    out.toByteArray
  }

  override protected def doGenCode(
    ctx: CodegenContext,
    ev: ExprCode
  ): ExprCode = {
    ev.isNull = "false"
    val out = ctx.freshName("out")
    val serializeChildren = children.map { child =>
      val childEval = child.genCode(ctx)
      s"""|${childEval.code}
          |if (!${childEval.isNull}) {
          |  ${Serialize.getClass.getName.dropRight(1)}.write(${childEval.value}, $out);
          |} else {
          |  ${Serialize.getClass.getName.dropRight(1)}.throwNull("$child of type ${child.getClass.getSimpleName}");
          |}""".stripMargin
    }.mkString("\n")
    val baos = classOf[ByteArrayOutputStream].getName
    ev.copy(
      s"""|$baos $out = new $baos();
          |$serializeChildren
          |byte[] ${ev.value} = $out.toByteArray();""".stripMargin)
  }

}

object Serialize {

  @inline private final def writeLengthAndValue(value: Array[Byte], out: ByteArrayOutputStream): Unit = {
    out.write(encodeLong(value.length))
    out.write(value)
  }

  @inline final def write(value: Array[Byte], out: ByteArrayOutputStream): Unit = writeLengthAndValue(value, out)

  @inline final def write(
    value: Boolean,
    out: ByteArrayOutputStream
  ): Unit = write(if (value) 1.toLong else 0.toLong, out)

  @inline final def write(value: Byte, out: ByteArrayOutputStream): Unit = write(value.toLong, out)

  @inline final def write(value: Int, out: ByteArrayOutputStream): Unit = write(value.toLong, out)

  @inline final def write(value: Long, out: ByteArrayOutputStream): Unit = writeLengthAndValue(encodeLong(value), out)

  @inline final def write(
    value: UTF8String,
    out: ByteArrayOutputStream
  ): Unit = writeLengthAndValue(value.getBytes, out)

  @inline final def write(value: String, out: ByteArrayOutputStream): Unit = writeLengthAndValue(value.getBytes, out)

  // TODO: Enable check once bug in SQLPGDS is fixed
  final def throwNull(childDescription: String): Unit = {
//    throw exception.IllegalStateException(
//      s"""|Columns that are serialized as part of a node/relationship key cannot be null.
//          |Expression `$childDescription` evaluated to null.
//      """.stripMargin)
  }

}
