/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencypher.spark.api.ir.block

import org.opencypher.spark.api.ir.Field
import org.opencypher.spark.api.ir.pattern.AllGiven

import scala.language.implicitConversions

trait Block[E] {
  def blockType: BlockType
  def isLeaf: Boolean = after.isEmpty

  def after: Set[BlockRef]

  def binds: Binds[E]
  def where: AllGiven[E]

  def graph: BlockRef
}

final case class BlockType(name: String)

trait Binds[E] {
  def fields: Set[Field]
}

object BlockWhere {
  def unapply[E](block: Block[E]): Option[Set[E]] = Some(block.where.elts)
}

object NoWhereBlock {
  def unapply[E](block: Block[E]): Option[Block[E]] =
    if (block.where.elts.isEmpty) Some(block) else None
}


