package org.opencypher.okapi.impl.util

import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.scalatest.{FunSpec, Matchers}

class VersionTest extends FunSpec with Matchers {
  describe("parsing") {
    it("parses two valued version numbers") {
      Version("1.0") should equal(Version(1,0))
      Version("1.5") should equal(Version(1,5))
      Version("42.21") should equal(Version(42,21))
    }

    it("parses single valued version numbers") {
      Version("1") should equal(Version(1,0))
      Version("42") should equal(Version(42,0))
    }

    it("throws errors on malformed version string") {
      an[IllegalArgumentException] shouldBe thrownBy(Version("foo"))
      an[IllegalArgumentException] shouldBe thrownBy(Version("1.foo"))
      an[IllegalArgumentException] shouldBe thrownBy(Version("foo.bar"))
    }
  }

  describe("toString") {
    it("turns into a version string") {
      Version("1.5").toString shouldEqual "1.5"
      Version("2").toString shouldEqual "2.0"
    }
  }

  describe("compatibleWith") {
    it("is compatible with it self") {
      Version("1.5").compatibleWith(Version("1.5")) shouldBe true
    }

    it("is compatible if the major versions match") {
      Version("1.5").compatibleWith(Version("1.0")) shouldBe true
      Version("1.0").compatibleWith(Version("1.5")) shouldBe true
    }

    it("is incompatible if the major versions differ") {
      Version("1.5").compatibleWith(Version("2.5")) shouldBe false
      Version("2.0").compatibleWith(Version("1.5")) shouldBe false
    }
  }
}
