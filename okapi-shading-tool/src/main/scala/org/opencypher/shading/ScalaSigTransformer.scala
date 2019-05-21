/*
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
package org.opencypher.shading

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

import org.opencypher.shading.ScalaSigTransformer._

import scala.reflect.internal.pickling.{ByteCodecs, PickleFormat}
import scala.tools.asm.tree.AnnotationNode
import scala.tools.asm.{AnnotationVisitor, ClassVisitor, ClassWriter}
import scala.tools.nsc.util.ShowPickled
import scala.tools.scalap.scalax.rules.scalasig.{ByteCode, ExternalSymbol, ScalaSig, ScalaSigAttributeParsers}
import scala.tools.scalap.scalax.rules.~

class ScalaSigTransformer(api: Int, writer: ClassWriter) extends ClassVisitor(api, writer) {

  override def visitAnnotation(descriptor: String, visible: Boolean): AnnotationVisitor = {

    new AnnotationNode(api, descriptor) {

      override def visit(name: String, value: Any): Unit = {

        value match {
          case s: String =>
            val bytes = s.getBytes().clone()
            val length = ByteCodecs.decode(bytes)
            val sig: ScalaSig = ScalaSigAttributeParsers.parse(ByteCode(bytes.take(length)))

            sig.printComparison()

            writer.visitAnnotation(desc, visible).visit(name, sig.encodeToString)
          case _ =>
        }
      }
    }
  }
}

object ScalaSigTransformer {
  implicit class RichScalaSig(val sig: ScalaSig) extends AnyVal {
    def bytes: Array[Byte] = {
      val baos = new ByteArrayOutputStream()
      NatWriter.write(sig.majorVersion, baos)
      NatWriter.write(sig.minorVersion, baos)

      NatWriter.write(sig.table.size + 2, baos)
      sig.table.zipWithIndex.foreach {

        case (~(i@PickleFormat.EXTMODCLASSref, b), idx) =>
//          baos.write(PickleFormat.EXTMODCLASSref)
//          NatWriter.write(b.length, baos)
//          baos.write(b.bytes, b.pos, b.length)

//          val typeName = ShowPickled.tag2string(i)

          sig.parseEntry(idx) match {
            case symbol@ExternalSymbol(_, Some(_), _) =>
//              val foo = symbol
              baos.write(PickleFormat.EXTMODCLASSref)
              NatWriter.write(b.length, baos)  // always 2 references, one for the name and one for the symbol
              baos.write(b.bytes, b.pos, b.length) // TODO: should this be NAT?

            case symbol@ExternalSymbol(name, None, entry) if symbol.path.startsWith("cats") =>

              baos.write(PickleFormat.EXTMODCLASSref)
              NatWriter.write(2, baos)  // b.length is actually 1, but we want to insert a symbol reference here
              baos.write(b.bytes, b.pos, 1)
              baos.write(b.bytes.length)

            case symbol@ExternalSymbol(name, None, entry) =>

              baos.write(PickleFormat.EXTMODCLASSref)
              NatWriter.write(1, baos)  // b.length is actually 1, but we want to insert a symbol reference here
              baos.write(b.bytes, b.pos, 1)

            case x =>
              val foo = x
              assert(assertion = false, "Not supposed to happen")
              ???
          }

//          println(s"$idx: $typeName ($i) ; $value")

        case (~(i@PickleFormat.TERMname, _), idx) =>

          val typeName = ShowPickled.tag2string(i)

          sig.parseEntry(idx) match {
            case s: String if s.startsWith("cats") =>
              val relocated = s"org.opencypher.relocated.$s"

//              println(s"$idx: $typeName ($i) ; $relocated")

              val bytes = s.getBytes(StandardCharsets.UTF_8)
              baos.write(PickleFormat.TERMname)
              NatWriter.write(bytes.length, baos)
              baos.write(bytes)

            case s: String =>
//              println(s"$idx: $typeName ($i) ; $s")

              val bytes = s.getBytes(StandardCharsets.UTF_8)
              baos.write(PickleFormat.TERMname)
              NatWriter.write(bytes.length, baos)
              baos.write(bytes)

            case x =>
              assert(assertion = false, "Not supposed to happen")
              ???
          }

        case (~(i, b), idx) =>
          baos.write(i)
          NatWriter.write(b.length, baos)
          baos.write(b.bytes, b.pos, b.length)

//          val typeName = ShowPickled.tag2string(i)
//          val payload = sig.parseEntry(idx)
//          println(s"$idx: $typeName ($i) ; $payload")
      }

      // Write a new top-level parent ExtModClassRef to end the chain correctly
      baos.write(PickleFormat.EXTMODCLASSref)
      val tableIndex = sig.table.size + 1
      val natEncodingSize = (tableIndex >> 7) + 1 // how many bytes do we need to encode the index (at least 1)
      NatWriter.write(natEncodingSize, baos)
      NatWriter.write(tableIndex, baos)

      // Write a new termname for the extra package
      val newTermName = "relocated".getBytes(StandardCharsets.UTF_8)
      baos.write(PickleFormat.TERMname)
      NatWriter.write(newTermName.length, baos)
      baos.write(newTermName)


      baos.toByteArray
    }

    def encode: Array[Byte] = ByteCodecs.encode(bytes)

    def encodeToString: String = new String(encode)

    def printComparison(): Unit = {
      val _bytes = bytes
      val s = ScalaSigAttributeParsers.parse(ByteCode(_bytes.take(_bytes.length)))
      println(sig.toString())
      println(s.toString())
      println(sig.toString() == s.toString())
    }
  }
}
