package org.opencypher.memcypher.impl.cyphertable

import org.opencypher.memcypher.impl.records.MemCypherRecordsBehaviour
import org.opencypher.memcypher.impl.table.Table
import org.opencypher.okapi.api.io.conversion.RelationshipMapping
import org.opencypher.okapi.relational.api.io.RelationshipTable

case class MemRelationshipTable(
  override val mapping: RelationshipMapping,
  override val table: Table
) extends RelationshipTable(mapping, table) with MemCypherRecordsBehaviour {

  override type Records = MemRelationshipTable

  override def cache(): MemRelationshipTable = this
}
