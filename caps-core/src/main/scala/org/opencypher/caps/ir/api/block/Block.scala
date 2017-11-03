/*
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
package org.opencypher.caps.ir.api.block

import org.opencypher.caps.ir.api.pattern.AllGiven
import org.opencypher.caps.ir.api.{IRField, IRGraph, IRNamedGraph}

import scala.language.implicitConversions

trait Block[E] {
  def blockType: BlockType
  def isLeaf: Boolean = after.isEmpty

  def after: Set[BlockRef]

  def binds: Binds[E]
  def where: AllGiven[E]

  def source: IRGraph
}

trait UpdatingBlock[E] {
  self: Block[E] =>

  def target: BlockRef
}

final case class BlockType(name: String)

object Binds {
  def empty[E] = new Binds[E] {
    override def fields: Set[IRField] = Set.empty
  }
}

trait Binds[E] {
  def fields: Set[IRField]
  def graphs: Set[IRGraph] = Set.empty
}

object BlockWhere {
  def unapply[E](block: Block[E]): Option[Set[E]] = Some(block.where.elements)
}

object NoWhereBlock {
  def unapply[E](block: Block[E]): Option[Block[E]] =
    if (block.where.elements.isEmpty) Some(block) else None
}


