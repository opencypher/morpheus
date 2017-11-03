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
package org.opencypher.caps.ir.impl

import org.opencypher.caps.ir.api.block.{Block, BlockRef, BlockType}

object BlockRegistry {
  def empty[E]: BlockRegistry[E] = BlockRegistry[E](Seq.empty)()
}

// TODO: Make this inherit from Register
case class BlockRegistry[E](reg: Seq[(BlockRef, Block[E])])(private val counter: Int = 0) {

  def register(blockDef: Block[E]): (BlockRef, BlockRegistry[E]) = {
    val ref = BlockRef(generateName(blockDef.blockType))
    ref -> copy(reg = reg :+ ref -> blockDef)(counter + 1)
  }

  def apply(ref: BlockRef): Block[E] =
    reg
      .find {
        case (_ref, b) => ref == _ref
      }
      .getOrElse(throw new NoSuchElementException(s"Didn't find block with reference $ref"))
      ._2

  def lastAdded: Option[BlockRef] = reg.lastOption.map(_._1)

  private def generateName(t: BlockType) = s"${t.name}_$counter"
}
