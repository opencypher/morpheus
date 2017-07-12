package org.opencypher.spark.impl

package object syntax {
  object all extends AllSyntax
  object cypher extends CypherSyntax
  object register extends RegisterSyntax
  object header extends RecordHeaderSyntax
  object expr extends ExprSyntax
  object block extends BlockSyntax
}
