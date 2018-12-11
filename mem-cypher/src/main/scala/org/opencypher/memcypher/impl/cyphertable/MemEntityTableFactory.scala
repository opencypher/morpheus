package org.opencypher.memcypher.impl.cyphertable

import org.opencypher.memcypher.impl.table.Table
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.okapi.relational.api.io.{NodeTable, RelationshipTable}
import org.opencypher.okapi.relational.api.table.RelationalEntityTableFactory

case object MemEntityTableFactory extends RelationalEntityTableFactory[Table] {

  override def nodeTable(
    nodeMapping: NodeMapping,
    table: Table
  ): NodeTable[Table] = MemNodeTable(nodeMapping, table)

  override def relationshipTable(
    relationshipMapping: RelationshipMapping,
    table: Table
  ): RelationshipTable[Table] = MemRelationshipTable(relationshipMapping, table)
}
