package org.opencypher.okapi.ir.impl.typer

import org.opencypher.okapi.api.types._
import org.opencypher.okapi.ir.impl.typer.SignatureConverter.FunctionSignatures
import org.opencypher.okapi.testing.BaseTestSuite
import org.opencypher.v9_0.expressions.functions.{Abs, _}

class SignatureConverterTest extends BaseTestSuite {

  def expected(x: Any) = x match {
    case Min | Max => Set(
      FunctionSignature(Seq(CTNull), CTNull),
      FunctionSignature(Seq(CTInteger), CTInteger),
      FunctionSignature(Seq(CTFloat), CTFloat)
    )

    case Id => Set(
      FunctionSignature(Seq(CTNull), CTIdentity.nullable),
      FunctionSignature(Seq(CTNode), CTIdentity),
      FunctionSignature(Seq(CTRelationship), CTIdentity)
    )

    case ToBoolean => Set(
      FunctionSignature(Seq(CTNull), CTNull),
      FunctionSignature(Seq(CTString), CTBoolean.nullable),
      FunctionSignature(Seq(CTBoolean), CTBoolean)
    )

    case Sqrt | Log | Log10 | Exp | Ceil | Floor | Round => Set(
      FunctionSignature(Seq(CTNull), CTNull),
      FunctionSignature(Seq(CTFloat), CTFloat),
      FunctionSignature(Seq(CTInteger), CTFloat)
    )

    case Abs => Set(
      FunctionSignature(Seq(CTNull), CTNull),
      FunctionSignature(Seq(CTFloat), CTFloat),
      FunctionSignature(Seq(CTInteger), CTInteger)
    )

    case Sign => Set(
      FunctionSignature(Seq(CTNull), CTNull),
      FunctionSignature(Seq(CTFloat), CTInteger),
      FunctionSignature(Seq(CTInteger), CTInteger)
    )

    case Acos | Asin | Atan | Cos | Cot | Degrees | Haversin | Radians | Sin | Tan => Set(
      FunctionSignature(Seq(CTNull), CTNull),
      FunctionSignature(Seq(CTFloat), CTFloat),
      FunctionSignature(Seq(CTInteger), CTFloat)
    )

    case Atan2 => Set(
      FunctionSignature(Seq(CTNull, CTNull), CTNull),
      FunctionSignature(Seq(CTNull, CTFloat), CTNull),
      FunctionSignature(Seq(CTFloat, CTNull), CTNull),
      FunctionSignature(Seq(CTFloat, CTFloat), CTFloat),
      FunctionSignature(Seq(CTFloat, CTInteger), CTFloat),
      FunctionSignature(Seq(CTInteger, CTFloat), CTFloat),
      FunctionSignature(Seq(CTInteger, CTInteger), CTFloat),
      FunctionSignature(Seq(CTNull, CTInteger), CTNull),
      FunctionSignature(Seq(CTInteger, CTNull), CTNull)
    )
  }


  it("Sign") {
    val functions = Seq(
      Min, Max,
      Id,
      ToBoolean,
      Sqrt, Log, Log10, Exp, Ceil, Floor, Round,
      Abs,
      Sign,
      Acos, Asin, Atan, Cos, Cot, Degrees, Haversin, Radians, Sin, Tan,
      Atan2
    )

    val sigs =
      FunctionSignatures
        .from(Atan2)
        .expandWithReplacement(CTFloat, CTInteger)
        .expandWithNulls
        .sigs.distinct

    sigs.toSeq.foreach(println)

    //sigs shouldEqual Set(
    //  FunctionSignature(Seq(CTInteger), CTInteger),
    //  FunctionSignature(Seq(CTFloat), CTInteger),
    //  FunctionSignature(Seq(CTNull), CTNull)
    //)
  }
}
