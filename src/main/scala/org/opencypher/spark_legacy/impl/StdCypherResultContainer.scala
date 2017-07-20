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
package org.opencypher.spark_legacy.impl

import java.io.PrintWriter

import org.apache.spark.sql.Row
import org.opencypher.spark_legacy.api.{CypherResult, CypherResultContainer}
import org.opencypher.spark_legacy.impl.frame.{ProductAsRow, RowAsProduct}
import org.opencypher.spark.api.value.CypherValue

object StdCypherResultContainer {

  def fromRows(rowFrame: StdCypherFrame[Row])(implicit planningContext: PlanningContext, runtimeContext: StdRuntimeContext): CypherResultContainer = {
    new StdCypherResultContainer(rowFrame, RowAsProduct(rowFrame))
  }

  def fromProducts(productFrame: StdCypherFrame[Product])(implicit planningContext: PlanningContext, runtimeContext: StdRuntimeContext): CypherResultContainer = {
    new StdCypherResultContainer(ProductAsRow(productFrame), productFrame)
  }
}

final class StdCypherResultContainer(rowFrame: StdCypherFrame[Row], productFrame: StdCypherFrame[Product])
                                    (implicit val context: StdRuntimeContext) extends CypherResultContainer {

  override lazy val rows: CypherResult[Row] = new StdRowResult(rowFrame)
  override lazy val products: CypherResult[Product] = new StdProductResult(productFrame)
  override lazy val records: CypherResult[Map[String, CypherValue]] = new StdRecordResult(productFrame)

  override def print(writer: PrintWriter): Unit = {
    val fields = records.signature.fields
    val lineWidth = 20 * fields.size + 5
    val --- = "+" + repeat("-", lineWidth) + "+"

    writer.println(---)
    var sep = "| "
    fields.foreach { f =>
      writer.print(sep)
      writer.print(fitTo20(f.sym.name))
      sep = " | "
    }
    writer.println(" |")
    writer.println(---)
    records.collectAsScalaList.foreach { r =>
      sep = "| "
      r.foreach {
        case (_, value) =>
          writer.print(sep)
          writer.print(fitTo20(value.toString))
          sep = " | "
      }
      writer.println(" |")
    }
    writer.println(---)
    writer.flush()
  }

  private def repeat(x: String, size: Int): String = (1 to size).map((_) => x).mkString
  private def fitTo20(s: String) = (s + "                    ").take(20)
}
