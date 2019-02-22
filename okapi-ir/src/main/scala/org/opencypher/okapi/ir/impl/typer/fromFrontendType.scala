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
package org.opencypher.okapi.ir.impl.typer

import org.opencypher.okapi.api.types._
import org.opencypher.v9_0.expressions.{TypeSignature, TypeSignatures}
import org.opencypher.v9_0.util.{symbols => frontend}

import scala.collection.immutable.ListSet

case object fromFrontendType extends (frontend.CypherType => Option[CypherType]) {
  override def apply(in: frontend.CypherType): Option[CypherType] = in match {
    case frontend.CTAny           => Some(CTAny)
    case frontend.CTNumber        => Some(CTNumber)
    case frontend.CTInteger       => Some(CTInteger)
    case frontend.CTFloat         => Some(CTFloat)
    case frontend.CTBoolean       => Some(CTBoolean)
    case frontend.CTString        => Some(CTString)
    case frontend.CTNode          => Some(CTNode)
    case frontend.CTRelationship  => Some(CTRelationship)
    case frontend.CTPath          => Some(CTPath)
    case frontend.CTLocalDateTime => Some(CTLocalDateTime)
    case frontend.CTDate          => Some(CTDate)
    case frontend.CTDuration      => Some(CTDuration)
    case frontend.CTMap           => Some(CTMap(Map.empty)) // TODO: this is not very correct
    case frontend.ListType(inner) =>
      fromFrontendType(inner) match {
        case None => None
        case Some(t) => Some(CTList(t))
      }
    case _                        => None
  }
}


case class FunctionSignature(input: Seq[CypherType], output: CypherType)

object SignatureConverter {

  implicit class RichTypeSignature(val frontendSig: TypeSignature) extends AnyVal{
    def convert: Option[FunctionSignature] = {
      val inTypes = frontendSig.argumentTypes.map(fromFrontendType)
      val outType = fromFrontendType(frontendSig.outputType)
      if (inTypes.contains(None) || outType.isEmpty)
        None
      else {
        val sigInputTypes = inTypes.flatten.map(_.nullable)
        // we don't know exactly if this is nullable from the frontend, but nullable is a safe superset
        val sigOutputType = outType.get.nullable
        Some(FunctionSignature(sigInputTypes, sigOutputType))
      }
    }

    def convertDirect: Option[FunctionSignature] = {
      val argumentTypes = frontendSig.argumentTypes.map(fromFrontendType)
      val outputType = fromFrontendType(frontendSig.outputType)
      (argumentTypes, outputType) match {
        case (argTypes, Some(outType)) if argTypes.forall(_.isDefined) =>
          Some(FunctionSignature(argTypes.flatten, outType))
        case _ =>
          None
      }
    }
  }

  def mask(size: Int, hits: Int) =
    Seq.fill(hits)(true) ++ Seq.fill(size - hits)(false)

  def masks(size: Int, minHits: Int, maxHits: Int) = for {
      hits <- Range.inclusive(minHits, maxHits)
      mask <- mask(size, hits).permutations
    } yield mask

  def substituteMasked[T](seq: Seq[T], mask: Int => Boolean)(sub: T => T) = for {
      (orig, i) <- seq.zipWithIndex
    } yield if (mask(i)) sub(orig) else orig

  def substitutions[T](seq: Seq[T], minSubs: Int, maxSubs: Int)(sub: T => T): Seq[Seq[T]] =
    masks(seq.size, minSubs, maxSubs).map(mask => substituteMasked(seq, mask)(sub))

  object FunctionSignatures {
    def from(original: TypeSignatures): FunctionSignatures =
      FunctionSignatures(original.signatures.flatMap(_.convertDirect))
  }

  case class FunctionSignatures(sigs: Seq[FunctionSignature]) {

    def include(added: Seq[FunctionSignature]): FunctionSignatures =
      FunctionSignatures(sigs ++ added)

    def expandWithNulls: FunctionSignatures = include(for {
      signature <- sigs
      alternative <- substitutions(signature.input, 1, signature.input.size)(_ => CTNull)
    } yield FunctionSignature(alternative, CTNull))

    def expandWithNullable: FunctionSignatures = include(for {
      signature <- sigs
      alternative <- substitutions(signature.input, 1, signature.input.size)(_.nullable)
    } yield FunctionSignature(alternative, signature.output.nullable))

    def expandWithReplacement(old: CypherType, rep: CypherType): FunctionSignatures = include(for {
      signature <- sigs
      alternative <- substitutions(signature.input, 1, signature.input.size)(ct => if (ct == old) rep else old)
    } yield FunctionSignature(alternative, signature.output))
    lazy val signatures: Set[FunctionSignature] = ListSet(sigs: _*)
  }

  def signatures(original: TypeSignatures): Set[FunctionSignature] = {
    FunctionSignatures
      .from(original)
      .expandWithReplacement(CTFloat, CTInteger)
      .expandWithNulls
      .expandWithNullable
      .signatures

  }
    //for {
    //  orig <- original.signatures
    //  sig <- orig.convertDirect
    //  alt <- substitutions(sig.input, CTNull)
    //  foo = FunctionSignature()
    //}

}
