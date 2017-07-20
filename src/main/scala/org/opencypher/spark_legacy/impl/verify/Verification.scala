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
package org.opencypher.spark_legacy.impl.verify

import org.opencypher.spark_legacy.impl.error.{StdError, StdErrorInfo}
import org.opencypher.spark_legacy.impl.verify.Verification.SupposedlyImpossible

object Verification extends Verification {
  abstract class Error(override val detail: String)(implicit private val info: StdErrorInfo)
    extends StdError(detail) {
    self: Product with Serializable =>
  }

  final case class UnObtainable[A](arg: A)(implicit info: StdErrorInfo)
    extends Error(s"Cannot obtain ${info.enclosing} '$arg'")

  final case class SupposedlyImpossible(msg: String)(implicit info: StdErrorInfo)
    extends Error(msg)
}

trait Verification {

  protected def ifNot[T <: Verification.Error](cond: => Boolean): Verifier[T, Unit] =
    new Verifier(if (cond) Verifier.pass else Verifier.fail)

  protected def ifMissing[T <: Verification.Error, V](value: => Option[V]): Verifier[T, V] =
    new Verifier(value)

  protected def ifExists[T <: Verification.Error, V](value: => Option[V]): Verifier[T, Unit] =
    ifNot(value.isEmpty)

  protected def obtain[A, T](value: A => Option[T])(arg: A)(implicit info: StdErrorInfo): T =
    ifMissing(value(arg)) failWith Verification.UnObtainable(arg)(info)

  protected def supposedlyImpossible(msg: String)(implicit info: StdErrorInfo) =
    throw SupposedlyImpossible(msg)
}



