package org.opencypher.spark.impl.ir
import cats.data.State
import cats.implicits._
import org.atnos.eff._
import org.atnos.eff.all.{left, pure}
import org.atnos.eff.syntax.all._
import org.opencypher.spark.api.expr.Expr

package object types {

  type _fails[R] = Fails |= R
  type _hasContext[R] = HasContext |= R

  type Fails[A] = Either[IRBuilderError, A]
  type HasContext[A] = State[IRBuilderContext, A]

  type IRBuilderStack[A] = Fx.fx2[Fails, HasContext]

  implicit final class RichIRBuilderStack[A](val program: Eff[IRBuilderStack[A], A]) {

    def run(context: IRBuilderContext): Either[IRBuilderError, (A, IRBuilderContext)] = {
      val stateRun = program.runState(context)
      val errorRun = stateRun.runEither[IRBuilderError, NoFx]
      errorRun.run
    }
  }

  def error[R: _fails : _hasContext, A](err: IRBuilderError)(v: A): Eff[R, A] =
    left[R, IRBuilderError, BlockRegistry[Expr]](err) >> pure(v)
}
