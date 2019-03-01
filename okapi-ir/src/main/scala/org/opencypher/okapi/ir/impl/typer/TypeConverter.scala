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
import org.opencypher.okapi.ir.impl.parse.{functions => ext}
import org.neo4j.cypher.internal.v4_0.expressions.TypeSignature
import org.neo4j.cypher.internal.v4_0.util.{symbols => frontend}

import scala.collection.immutable.ListSet

object TypeConverter {
  def convert(in: frontend.CypherType): Option[CypherType] = in match {
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
    case ext.CTIdentity           => Some(CTIdentity)
    case frontend.CTMap           => Some(CTMap(Map.empty)) // TODO: this is not very correct
    case frontend.ListType(inner) =>
      TypeConverter.convert(inner) match {
        case None => None
        case Some(t) => Some(CTList(t))
      }
    case _                        => None
  }
}




object SignatureConverter {

  case class FunctionSignature(input: Seq[CypherType], output: CypherType)

  def convert(frontendSig: TypeSignature): Option[FunctionSignature] = {
    val argumentTypes = frontendSig.argumentTypes.map(TypeConverter.convert)
    val outputType = TypeConverter.convert(frontendSig.outputType)
    (argumentTypes, outputType) match {
      case (argTypes, Some(outType)) if argTypes.forall(_.isDefined) =>
        Some(FunctionSignature(argTypes.flatten, outType))
      case _ =>
        None
    }
  }

  def from(original: Seq[TypeSignature]): FunctionSignatures =
    FunctionSignatures(original.flatMap(convert))

  case class FunctionSignatures(sigs: Seq[FunctionSignature]) {

    def include(added: Seq[FunctionSignature]): FunctionSignatures =
      FunctionSignatures(sigs ++ added)

    def expandWithNulls: FunctionSignatures = include(for {
      signature <- sigs
      alternative <- substitutions(signature.input, 1, signature.input.size)(_ => CTNull)
    } yield FunctionSignature(alternative, CTNull))

    def expandWithSubstitutions(old: CypherType, rep: CypherType): FunctionSignatures = include(for {
      signature <- sigs
      alternative <- substitutions(signature.input, 1, signature.input.size)(replace(old, rep))
      if sigs.forall(_.input != alternative)
    } yield FunctionSignature(alternative, signature.output))

    lazy val signatures: Set[FunctionSignature] = ListSet(sigs: _*)

    private def mask(size: Int, hits: Int) =
      Seq.fill(hits)(true) ++ Seq.fill(size - hits)(false)

    private def masks(size: Int, minHits: Int, maxHits: Int) = for {
      hits <- Range.inclusive(minHits, maxHits)
      mask <- mask(size, hits).permutations
    } yield mask

    private def substituteMasked[T](seq: Seq[T], mask: Int => Boolean)(sub: T => T) = for {
      (orig, i) <- seq.zipWithIndex
    } yield if (mask(i)) sub(orig) else orig

    private def substitutions[T](seq: Seq[T], minSubs: Int, maxSubs: Int)(sub: T => T): Seq[Seq[T]] =
      masks(seq.size, minSubs, maxSubs).map(mask => substituteMasked(seq, mask)(sub))

    private def replace[T](old: T, rep: T)(t: T) =
      if (t == old) rep else t
  }

}
