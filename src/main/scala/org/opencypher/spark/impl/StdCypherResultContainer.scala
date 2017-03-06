package org.opencypher.spark.impl

import org.apache.spark.sql.Row
import org.opencypher.spark.api.{CypherResult, CypherResultContainer}
import org.opencypher.spark.impl.frame.{ProductAsRow, RowAsProduct}
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

  override def show(): Unit = {
    var sep = ""
    records.signature.fields.foreach { f =>
      print(sep)
      print(fitTo20(f.sym.name))
      sep = " | "
    }
    println()
    records.collectAsScalaList.foreach { r =>
      sep = ""
      r.foreach {
        case (_, value) =>
          print(fitTo20(value.toString))
          sep = " | "
      }
      println()
    }
  }

  private def fitTo20(s: String) = (s + "                    ").take(20)
}
