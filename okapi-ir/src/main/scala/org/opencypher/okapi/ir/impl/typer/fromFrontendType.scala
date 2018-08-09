/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
import org.opencypher.v9_0.expressions.TypeSignature
import org.opencypher.v9_0.util.{symbols => frontend}

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
    case frontend.CTMap           => Some(CTMap)
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
  }
}
