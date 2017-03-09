package org.opencypher.spark.impl

import java.io.PrintWriter

import org.apache.spark.sql.Row
import org.opencypher.spark.api.{CypherResult, CypherResultContainer}
import org.opencypher.spark.impl.frame.{ProductAsRow, RowAsProduct}
import org.opencypher.spark.prototype.api.value.CypherValue

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
