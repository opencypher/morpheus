package org.opencypher.spark.prototype.ir.impl

import org.opencypher.spark.prototype.ir.{Blocks, Field, RootBlock, TokenRegistry}
import org.opencypher.spark.prototype.{Param, Var}

case class RootBlockImpl[E](outputs: Set[Field], params: Set[Param], variables: Set[Var], tokens: TokenRegistry, blocks: Blocks[E]) extends RootBlock[E]
