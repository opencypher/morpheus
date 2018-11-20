package org.opencypher.memcypher.impl.graphs

import org.opencypher.memcypher.api.MemCypherSession
import org.opencypher.memcypher.impl.table.Table
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraphFactory

case class MemCypherGraphFactory(implicit val session: MemCypherSession)
  extends RelationalCypherGraphFactory[Table] {}
