package org.opencypher.memcypher.api

import org.opencypher.memcypher.impl.cyphertable.MemEntityTableFactory
import org.opencypher.memcypher.impl.graphs.MemCypherGraphFactory
import org.opencypher.memcypher.impl.records.{MemCypherRecords, MemCypherRecordsFactory}
import org.opencypher.memcypher.impl.table.Table
import org.opencypher.okapi.relational.api.graph.RelationalCypherSession

case class MemCypherSession() extends RelationalCypherSession[Table] {

  private implicit val session: MemCypherSession = this

  override type Records = MemCypherRecords

  override private[opencypher] def records: MemCypherRecordsFactory = MemCypherRecordsFactory()

  override private[opencypher] def graphs: MemCypherGraphFactory = MemCypherGraphFactory()

  override private[opencypher] def entityTables = MemEntityTableFactory
}
