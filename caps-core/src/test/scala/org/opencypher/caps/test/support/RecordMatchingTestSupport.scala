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
package org.opencypher.caps.test.support

import org.opencypher.caps.api.expr.Var
import org.opencypher.caps.api.record.{FieldSlotContent, OpaqueField, ProjectedExpr, RecordHeader}
import org.opencypher.caps.api.spark.CAPSRecords
import org.opencypher.caps.api.value.CypherMap
import org.opencypher.caps.test.BaseTestSuite
import org.opencypher.caps.test.fixture.SparkSessionFixture
import org.scalatest.Assertion

import scala.collection.Bag
import scala.collection.JavaConverters._
import scala.collection.immutable.HashedBagConfiguration

trait RecordMatchingTestSupport {

  self: BaseTestSuite with SparkSessionFixture =>

  implicit val bagConfig: HashedBagConfiguration[CypherMap] = Bag.configuration.compact[CypherMap]

  implicit class RecordMatcher(records: CAPSRecords) {
    def shouldMatch(expected: CypherMap*): Assertion = {
      records.toMaps should equal(Bag(expected: _*))
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
      val newSlots = records.header.slots.map(_.content).map {
        case slot: FieldSlotContent => OpaqueField(slot.field)
        case slot: ProjectedExpr => OpaqueField(Var(slot.expr.withoutType)(slot.cypherType))
      }
      val newHeader = RecordHeader.from(newSlots: _*)
      val newData = records.data.toDF(newHeader.internalHeader.columns: _*)
      CAPSRecords.create(newHeader, newData)(records.caps)
    }
  }

  implicit class RichRecords(records: CAPSRecords) {
    import org.opencypher.caps.impl.spark.RowUtils._

    def toMaps: Bag[CypherMap] = {
      val rows = records.toDF().collect().map { r =>
        val properties = records.header.slots.map { s =>
          s.content match {
            case f: FieldSlotContent => f.field.name -> r.getCypherValue(f.key, records.header)
            case x => x.key.withoutType -> r.getCypherValue(x.key, records.header)
          }
        }.toMap
        CypherMap(properties)
      }
      Bag(rows: _*)
    }
  }
}
