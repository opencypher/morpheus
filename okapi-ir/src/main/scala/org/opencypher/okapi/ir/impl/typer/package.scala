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
package org.opencypher.okapi.ir.impl

import cats.Show
import cats.data._
import org.atnos.eff._
import org.atnos.eff.all._
import org.atnos.eff.syntax.all._
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.ir.impl.exception.TypingException
import org.neo4j.cypher.internal.v4_0.expressions.Expression

package object typer {

  type _keepsErrors[R] = KeepsErrors |= R
  type _hasTracker[R] = HasTracker |= R
  type _hasSchema[R] = HasSchema |= R
  type _logsTypes[R] = LogsTypes |= R

  type KeepsErrors[A] = Validate[TyperError, A]
  type HasTracker[A] = State[TypeTracker, A]
  type HasSchema[A] = Reader[Schema, A]
  type LogsTypes[A] = Writer[(Expression, CypherType), A]

  type TyperStack[A] = Fx.fx4[HasSchema, KeepsErrors, LogsTypes, HasTracker]

  implicit final class RichTyperStack[A](val program: Eff[TyperStack[A], A]) extends AnyVal {

    def runOrThrow(schema: Schema): TyperResult[A] =
      run(schema) match {
        case Left(failures) =>
          throw TypingException(s"Errors during schema-based expression typing: ${failures.toList.mkString(", ")}", Some(failures.head))

        case Right(result) =>
          result
      }

    def run(schema: Schema): Either[NonEmptyList[TyperError], TyperResult[A]] = {
      val rawResult: ((Either[NonEmptyList[TyperError], A], List[(Expression, CypherType)]), TypeTracker) = program
        .runReader(schema)
        .runNel[TyperError]
        .runWriter[(Expression, CypherType)]
        .runState(TypeTracker.empty)
        .run

      rawResult match {
        case ((Left(errors), _), _) => Left(errors)
        case ((Right(value), recordedTypes), tracker) =>
          Right(TyperResult(value, TypeRecorder.from(recordedTypes), tracker))
      }
    }
  }

  def parameterType[R: _hasTracker: _keepsErrors](it: String): Eff[R, CypherType] =
    for {
      tracker <- get[R, TypeTracker]
      cypherType <- tracker.getParameterType(it) match {
        case None    => error(UnTypedParameter(it)) >> pure[R, CypherType](CTAny)
        case Some(t) => pure[R, CypherType](t)
      }
    } yield cypherType

  def typeOf[R: _hasTracker: _keepsErrors](it: Expression): Eff[R, CypherType] =
    for {
      tracker <- get[R, TypeTracker]
      cypherType <- tracker.get(it) match {
        case None    => error(UnTypedExpr(it)) >> pure[R, CypherType](CTAny)
        case Some(t) => pure[R, CypherType](t)
      }
    } yield cypherType

  def recordAndUpdate[R: _hasTracker: _logsTypes](entry: (Expression, CypherType)): Eff[R, CypherType] =
    recordType(entry) >> updateTyping(entry)

  def updateTyping[R: _hasTracker](entry: (Expression, CypherType)): Eff[R, CypherType] = {
    val (expr, cypherType) = entry
    for {
      tracker <- get[R, TypeTracker]
      _ <- put[R, TypeTracker](tracker.updated(expr, cypherType))
    } yield cypherType
  }

  def recordType[R: _logsTypes](entry: (Expression, CypherType)): Eff[R, Unit] = {
    tell[R, (Expression, CypherType)](entry)
  }

  def recordTypes[R: _logsTypes](entries: (Expression, CypherType)*): Eff[R, Unit] = {
    entries.map(entry => tell[R, (Expression, CypherType)](entry)).reduce(_ >> _)
  }

  def error[R: _keepsErrors](failure: TyperError): Eff[R, CypherType] =
    wrong[R, TyperError](failure) >> pure(CTAny)

  implicit val showExpr: Show[Expression] = new Show[Expression] {
    override def show(it: Expression): String = s"$it [${it.position}]"
  }
}
