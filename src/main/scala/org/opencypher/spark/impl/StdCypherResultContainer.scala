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
    println("Showing row results")
    rows.toDF.show()
    println("Showing product results")
    products.toDS.show()
  }
}
