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
package org.opencypher.okapi.ir.impl.parse.functions

import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.expressions.functions.{AggregatingFunction, Function}
import org.neo4j.cypher.internal.v4_0.expressions.functions
import org.neo4j.cypher.internal.v4_0.util.symbols._

case object FunctionExtensions {

  private val mappings: Map[String, Function with TypeSignatures] = Map(
    Timestamp.name -> Timestamp,
    LocalDateTime.name -> LocalDateTime,
    Date.name -> Date,
    Duration.name -> Duration,
    ToBoolean.name -> ToBoolean,
    Min.name -> Min,
    Max.name -> Max,
    Id.name -> Id
  ).map(p => p._1.toLowerCase -> p._2)

  def get(name: String): Option[Function with TypeSignatures] =
    mappings.get(name.toLowerCase())
}

case object Timestamp extends Function with TypeSignatures {
  override val name = "timestamp"

  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(), outputType = CTInteger)
  )
}

case object LocalDateTime extends Function with TypeSignatures {
  override val name = "localdatetime"

  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTString), outputType = CTLocalDateTime),
    TypeSignature(argumentTypes = Vector(CTMap), outputType = CTLocalDateTime),
    TypeSignature(argumentTypes = Vector(), outputType = CTLocalDateTime)
  )
}

case object Date extends Function with TypeSignatures {
  override val name = "date"

  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTString), outputType = CTDate),
    TypeSignature(argumentTypes = Vector(CTMap), outputType = CTDate),
    TypeSignature(argumentTypes = Vector(), outputType = CTDate)

  )
}

case object Duration extends Function with TypeSignatures {
  override val name = "duration"

  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTString), outputType = CTDuration),
    TypeSignature(argumentTypes = Vector(CTMap), outputType = CTDuration)
  )
}

case object ToBoolean extends Function with TypeSignatures {
  override val name = functions.ToBoolean.name

  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTString), outputType = CTBoolean),
    TypeSignature(argumentTypes = Vector(CTBoolean), outputType = CTBoolean)
  )
}

case object Min extends AggregatingFunction with TypeSignatures {
  override def name: String = functions.Min.name

  override val signatures: Vector[TypeSignature] = Vector(
    TypeSignature(argumentTypes = Vector(CTFloat), outputType = CTFloat),
    TypeSignature(argumentTypes = Vector(CTInteger), outputType = CTInteger)
  )
}

case object Max extends AggregatingFunction with TypeSignatures {
  override def name: String = functions.Max.name

  override val signatures: Vector[TypeSignature] = Vector(
    TypeSignature(argumentTypes = Vector(CTFloat), outputType = CTFloat),
    TypeSignature(argumentTypes = Vector(CTInteger), outputType = CTInteger)
  )
}

object CTIdentity extends CypherType {
  override def parentType: CypherType = CTAny
  override def toNeoTypeString: String = "IDENTITY"
}

case object Id extends Function with TypeSignatures {
  def name: String = functions.Id.name

  override val signatures: Vector[TypeSignature] = Vector(
    TypeSignature(argumentTypes = Vector(CTNode), outputType = CTIdentity),
    TypeSignature(argumentTypes = Vector(CTRelationship), outputType = CTIdentity)
  )
}
