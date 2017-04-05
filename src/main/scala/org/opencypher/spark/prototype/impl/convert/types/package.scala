package org.opencypher.spark.prototype.impl.convert
import cats.Eval
import cats.data.{State, StateT}
import org.atnos.eff.syntax.all._
import org.atnos.eff._

package object types {

  type _fails[R] = Fails |= R
  type _hasContext[R] = HasContext |= R

  type Fails[A] = Either[IRBuilderError, A]
  type HasContext[A] = State[IRBuilderContext, A]

  type IRBuilderStack[A] = Fx.fx2[Fails, HasContext]

  implicit final class RichIRBuilderStack[A](val program: Eff[IRBuilderStack[A], A]) {

    def run(context: IRBuilderContext): Either[IRBuilderError, (A, IRBuilderContext)] = {

      val stateRun = program.runState(context)
      val errorRun: Eff[NoFx, Either[IRBuilderError, (A, IRBuilderContext)]] = stateRun.runEither[IRBuilderError, NoFx]

//      val foo: Eff[Fx1[StateT[Eval, IRBuilderContext, _]], Either[IRBuilderError, A]] = program.runEither
//      val result = foo.runState(context).run

      errorRun.run
//      match {
//        case Left(error) => throw new IllegalStateException(s"Whoopsie: $error")
//        case Right((r, c)) => r
//      }
    }
  }

}
