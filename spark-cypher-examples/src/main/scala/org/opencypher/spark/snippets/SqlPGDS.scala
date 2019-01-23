package org.opencypher.spark.snippets

import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.api.io.sql.SqlDataSourceConfig
import org.opencypher.spark.util.ConsoleApp

object SqlPGDS extends ConsoleApp {
  implicit val session: CAPSSession = CAPSSession.local()

  // tag::create-sql-pgds[]
  val sqlPgds = GraphSources
    .sql(resource("snippets/SqlPGDS.ddl").getFile)
    .withSqlDataSourceConfigs(
      "myHiveSource" -> SqlDataSourceConfig.Hive,
      "myH2Source" -> SqlDataSourceConfig.Jdbc(
        url = "jdbc:h2:mem:myH2.db;",
        driver = "org.h2.Driver"
      )
    )
  // end::create-sql-pgds[]

}
