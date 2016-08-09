package org.opencypher.spark.impl

import org.apache.spark.sql.Row
import org.opencypher.spark.CypherValue
import org.opencypher.spark.api.{CypherResult, CypherResultContainer}
import org.opencypher.spark.impl.frame.{ProductsAsRows, RowsAsProducts}

object StdCypherResultContainer {

  def fromRows(rowFrame: StdCypherFrame[Row])(implicit planningContext: PlanningContext, runtimeContext: StdRuntimeContext): CypherResultContainer = {
    new StdCypherResultContainer(rowFrame, RowsAsProducts(rowFrame))
  }

  def fromProducts(productFrame: StdCypherFrame[Product])(implicit planningContext: PlanningContext, runtimeContext: StdRuntimeContext): CypherResultContainer = {
    new StdCypherResultContainer(ProductsAsRows(productFrame), productFrame)
  }
}

final class StdCypherResultContainer(rowFrame: StdCypherFrame[Row], productFrame: StdCypherFrame[Product])
                                    (implicit val context: StdRuntimeContext) extends CypherResultContainer {

  override lazy val rows: CypherResult[Row] = new StdRowResult(rowFrame)
  override lazy val products: CypherResult[Product] = new StdProductResult(productFrame)
  override lazy val maps: CypherResult[Map[String, CypherValue]] = new StdMapResult(productFrame)

  override def show(): Unit = {
    println("Showing row results")
    rows.show()
    println("Showing product results")
    products.show()
  }
}
