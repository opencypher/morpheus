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
package org.opencypher.spark.impl

import cats.data.State
import cats.syntax.flatMap._
import org.atnos.eff._
import org.atnos.eff.all._
import org.atnos.eff.syntax.all._
import org.opencypher.spark.api.expr.Expr

package object ir {

  type _mayFail[R] = MayFail |= R
  type _hasContext[R] = HasContext |= R

  type MayFail[A] = Either[IRBuilderError, A]
  type HasContext[A] = State[IRBuilderContext, A]

  type IRBuilderStack[A] = Fx.fx2[MayFail, HasContext]

  implicit final class RichIRBuilderStack[A](val program: Eff[IRBuilderStack[A], A]) {

    def run(context: IRBuilderContext): Either[IRBuilderError, (A, IRBuilderContext)] = {
      val stateRun = program.runState(context)
      val errorRun = stateRun.runEither[IRBuilderError]
      errorRun.run
    }
  }

  def error[R: _mayFail : _hasContext, A](err: IRBuilderError)(v: A): Eff[R, A] =
    left[R, IRBuilderError, BlockRegistry[Expr]](err) >> pure(v)
}
