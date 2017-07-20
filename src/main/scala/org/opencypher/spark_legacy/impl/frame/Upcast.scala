/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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
 */
package org.opencypher.spark_legacy.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark_legacy.impl._
import org.opencypher.spark.api.types.CypherType

import scala.language.postfixOps

object Upcast extends FrameCompanion {

  def apply[Out](input: StdCypherFrame[Out])(fieldSym: Symbol)(widen: CypherType => CypherType)
                (implicit context: PlanningContext): StdCypherFrame[Out] = {

    val field = obtain(input.signature.field)(fieldSym)
    val oldType = field.cypherType
    val newType = widen(oldType)

    requireIsSuperTypeOf(newType, oldType)

    val (_, sig) = input.signature.upcastField(field.sym -> newType)
    CypherUpcast[Out](input)(sig)
  }

  private final case class CypherUpcast[Out](input: StdCypherFrame[Out])(sig: StdFrameSignature)
    extends StdCypherFrame[Out](sig) {

    override def execute(implicit context: RuntimeContext): Dataset[Out] = {
      val out = input.run
      out
    }
  }
}
