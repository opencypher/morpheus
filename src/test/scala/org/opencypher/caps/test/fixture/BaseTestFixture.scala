package org.opencypher.caps.test.fixture

import org.opencypher.caps.test.BaseTestSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

trait BaseTestFixture extends BeforeAndAfterEach with BeforeAndAfterAll {
  self: BaseTestSuite  =>
}
