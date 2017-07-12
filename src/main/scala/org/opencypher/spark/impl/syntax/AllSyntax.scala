package org.opencypher.spark.impl.syntax

trait AllSyntax
  extends CypherSyntax
  with RegisterSyntax
  with RecordHeaderSyntax
  with util.AllSyntax
  with BlockSyntax
