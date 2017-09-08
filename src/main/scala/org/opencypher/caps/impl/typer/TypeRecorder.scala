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
package org.opencypher.caps.impl.typer

import cats.kernel.Semigroup
import org.neo4j.cypher.internal.frontend.v3_3.Ref
import org.neo4j.cypher.internal.frontend.v3_3.ast.Expression
import org.opencypher.caps.api.types.CypherType

import scala.annotation.tailrec

final case class TypeRecorder(recordedTypes: List[(Ref[Expression], CypherType)]) {

  def toMap: Map[Ref[Expression], CypherType] = toMap(Map.empty, recordedTypes)

  @tailrec
  private def toMap(m: Map[Ref[Expression], CypherType],
                    recorded: Seq[(Ref[Expression], CypherType)]): Map[Ref[Expression], CypherType] =
    recorded.headOption match {
      case Some((ref, t)) =>
        m.get(ref) match {
          case Some(t2) => toMap(m.updated(ref, t.join(t2)), recorded.tail)
          case None => toMap(m.updated(ref, t), recorded.tail)
        }
      case None =>
        m
    }
}

object TypeRecorder {

  def from(tuples: List[(Expression, CypherType)]): TypeRecorder = {
    TypeRecorder(tuples.map {
      case (e, t) => Ref(e) -> t
    })
  }

  def single(entry: (Expression, CypherType)): TypeRecorder = {
    val (expr, cypherType) = entry
    TypeRecorder(List(Ref(expr) -> cypherType))
  }

  implicit object recorderSemigroup extends Semigroup[TypeRecorder] {
    override def combine(x: TypeRecorder, y: TypeRecorder): TypeRecorder = {
      TypeRecorder(x.recordedTypes ++ y.recordedTypes)
    }
  }

}
