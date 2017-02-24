package org.opencypher.spark.prototype.ir

import org.opencypher.spark.prototype.IrTestSuite
import org.opencypher.spark.prototype.ir.block._
import org.opencypher.spark.prototype.ir.pattern.AllGiven

class QueryModelTest extends IrTestSuite {

  val block_a = BlockRef("a")
  val block_b = BlockRef("b")
  val block_c = BlockRef("c")
  val block_d = BlockRef("d")
  val block_e = BlockRef("e")

  test("dependencies") {
    val model = irFor(block_a, Map(
      block_a -> DummyBlock(Set(block_b, block_c)),
      block_b -> DummyBlock(),
      block_c -> DummyBlock()
    )).model

    model.dependencies(block_a) should equal(Set(block_b, block_c))
    model.dependencies(block_b) should equal(Set.empty)
    model.dependencies(block_c) should equal(Set.empty)
  }

  test("all_dependencies") {
    val model = irFor(block_a, Map(
      block_a -> DummyBlock(Set(block_b, block_c)),
      block_b -> DummyBlock(Set(block_d)),
      block_c -> DummyBlock(),
      block_d -> DummyBlock(Set(block_e)),
      block_e -> DummyBlock()
    )).model

    model.allDependencies(block_a) should equal(Set(block_b, block_c, block_d, block_e))
    model.allDependencies(block_b) should equal(Set(block_d, block_e))
    model.allDependencies(block_c) should equal(Set.empty)
    model.allDependencies(block_d) should equal(Set(block_e))
    model.allDependencies(block_e) should equal(Set.empty)
  }

  test("handle loops") {
    val model = irFor(block_a, Map(
      block_a -> DummyBlock(Set(block_b, block_c)),
      block_b -> DummyBlock(Set(block_d)),
      block_c -> DummyBlock(Set(block_b)),
      block_d -> DummyBlock(Set(block_c))
    )).model

    an [IllegalStateException] shouldBe thrownBy {
      model.allDependencies(block_a)
    }
    an [IllegalStateException] shouldBe thrownBy {
      model.allDependencies(block_b)
    }
    an [IllegalStateException] shouldBe thrownBy {
      model.allDependencies(block_c)
    }
    an [IllegalStateException] shouldBe thrownBy {
      model.allDependencies(block_d)
    }
  }

  case class DummyBlock[E](after: Set[BlockRef] = Set.empty) extends BasicBlock[DummyBinds[E], E, BlockType] {
    override def blockType: BlockType = ???
    override def binds: DummyBinds[E] = DummyBinds[E]()
    override def over: BlockSig = BlockSig.empty
    override def where: AllGiven[E] = AllGiven[E]()
  }

  case class DummyBinds[E](fields: Set[Field] = Set.empty) extends Binds[E]
}
