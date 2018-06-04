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
package org.opencypher.okapi.relational.impl.syntax

import cats.data.State
import cats.data.State.{get, set}
import org.opencypher.okapi.relational.impl.table._

import scala.language.implicitConversions

object RecordHeaderSyntax extends RecordHeaderSyntax

trait RecordHeaderSyntax {

  implicit def sparkRecordHeaderSyntax(header: RecordHeader): RecordHeaderOps =
    new RecordHeaderOps(header)

  type HeaderState[X] = State[IRecordHeader, X]

  def addContents(contents: Seq[SlotContent]): State[RecordHeader, Vector[AdditiveUpdateResult[RecordSlot]]] =
    exec(InternalHeader.addContents(contents))

  def addContent(content: SlotContent): State[RecordHeader, AdditiveUpdateResult[RecordSlot]] =
    exec(InternalHeader.addContent(content))

  def compactFields(implicit details: RetainedDetails): State[RecordHeader, Vector[RemovingUpdateResult[RecordSlot]]] =
    exec(InternalHeader.compactFields)

  private def exec[O](inner: State[InternalHeader, O]): State[RecordHeader, O] =
    get[RecordHeader]
      .map(header => inner.run(header.internalHeader).value)
      .flatMap { case (newInternalHeader, value) => set(RecordHeader(newInternalHeader)).map(_ => value) }
}

final class RecordHeaderOps(header: RecordHeader) {

  def +(content: SlotContent): RecordHeader =
    header.copy(header.internalHeader + content)

  def update[A](ops: State[RecordHeader, A]): (RecordHeader, A) =
    ops.run(header).value
}
