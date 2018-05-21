package org.opencypher.okapi.impl.configuration

import org.scalatest.{FunSpec, FunSuite, Matchers}

class ConfigCachingTest extends FunSpec with Matchers {

  object TestConfigWithCaching extends ConfigFlag("test") with ConfigCaching[Boolean]

  it("can read a set value") {
    TestConfigWithCaching.set()
    TestConfigWithCaching.get shouldBe true
    TestConfigWithCaching.set("true")
    TestConfigWithCaching.get shouldBe true
    a[UnsupportedOperationException] shouldBe thrownBy {
      TestConfigWithCaching.set("false")
    }
  }

}
