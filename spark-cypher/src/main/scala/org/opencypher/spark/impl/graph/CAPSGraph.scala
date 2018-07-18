package org.opencypher.spark.impl.graph

import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.spark.impl.table.SparkFlatRelationalTable.DataFrameTable

object CAPSGraph {

  implicit class PersistenceOps(val graph: RelationalCypherGraph[DataFrameTable]) extends AnyVal {

    private def foreachDf(f: DataFrame => Unit): Unit = {
      graph.tables.foreach(t => f(t.df))
    }

    def persist(): RelationalCypherGraph[DataFrameTable] = {
      foreachDf(_.persist())
      graph
    }

    def persist(storageLevel: StorageLevel): RelationalCypherGraph[DataFrameTable] = {
      foreachDf(_.persist(storageLevel))
      graph
    }

    def unpersist(): RelationalCypherGraph[DataFrameTable] = {
      foreachDf(_.unpersist())
      graph
    }

    def unpersist(blocking: Boolean): RelationalCypherGraph[DataFrameTable] = {
      foreachDf(_.unpersist(blocking))
      graph
    }

  }

}
