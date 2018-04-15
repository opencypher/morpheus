/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.spark.test.support

import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.ir.test.support.Bag
import org.opencypher.okapi.ir.test.support.Bag._
import org.opencypher.okapi.relational.impl.table.RecordHeader._
import org.opencypher.okapi.relational.impl.table._
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.impl.CAPSRecords
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.impl.table.CAPSRecordHeader._
import org.opencypher.spark.test.CAPSTestSuite
import org.scalatest.Assertion

import scala.collection.JavaConverters._

trait RecordMatchingTestSupport {

  self: CAPSTestSuite =>

  implicit class RecordMatcher(records: CAPSRecords) {
    def shouldMatch(expected: CypherMap*): Assertion = {
      records.collect.toBag should equal(Bag(expected: _*))
    }

    def shouldMatch(expectedRecords: CAPSRecords): Assertion = {
      records.header should equal(expectedRecords.header)

      val actualData = records.toLocalIterator.asScala.toSet
      val expectedData = expectedRecords.toLocalIterator.asScala.toSet
      actualData should equal(expectedData)
    }

    def shouldMatchOpaquely(expectedRecords: CAPSRecords): Assertion = {
      RecordMatcher(projected(records)) shouldMatch projected(expectedRecords)
    }

    private def projected(records: CAPSRecords): CAPSRecords = {
      val newSlots: Set[SlotContent] = records.header.slots.map(_.content).map {
        case slot: FieldSlotContent => OpaqueField(slot.field)
        case slot: ProjectedExpr => OpaqueField(Var(slot.expr.withoutType)(slot.cypherType))
      }
      val newHeader = RecordHeader.fromSlotContents(newSlots)
      val alphabeticallyOrderedColumnNames = newHeader.fieldNames.toSeq.sorted
      val newData = records.data.toDF(alphabeticallyOrderedColumnNames: _*)
      CAPSRecords.verifyAndCreate(newHeader, newData)(records.caps)
    }
  }

  implicit class RichRecords(records: CypherRecords) {
    val capsRecords = records.asCaps

    // TODO: Remove this and replace usages with toMapsWithCollectedEntities below
    // probably use this name though, and have not collecting be the special case
    def toMaps: Bag[CypherMap] = {
      val df = capsRecords.toDF()
      val rows = df.collect().map { r =>
        val properties = capsRecords.header.slots.map { s =>
          s.content match {
            case f: FieldSlotContent => f.field.name -> r.getCypherValue(f.key, capsRecords.header, df)
            case x => x.key.withoutType -> r.getCypherValue(x.key, capsRecords.header, df)
          }
        }.toMap
        CypherMap(properties)
      }
      Bag(rows: _*)
    }

    def toMapsWithCollectedEntities: Bag[CypherMap] =
      Bag(capsRecords.toCypherMaps.collect(): _*)
  }

}
