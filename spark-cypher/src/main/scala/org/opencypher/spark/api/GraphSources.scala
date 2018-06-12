package org.opencypher.spark.api

import org.opencypher.spark.api.io.fs.FSGraphSource

object GraphSources {
  def fs = FSGraphSources
}

object FSGraphSources {
  def csv(rootPath: String)(implicit session: CAPSSession): FSGraphSource =
    new FSGraphSource(rootPath, "csv")

}
