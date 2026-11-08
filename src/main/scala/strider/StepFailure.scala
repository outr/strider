package strider

import fabric.{Json, Null}
import fabric.rw.*

/**
 * A step's failure in a form an application can read without parsing its message: `kind` names what failed (the
 * application's own vocabulary) and `detail` carries its fields. A step records one when the throwable it failed with,
 * or one of its causes, is a [[StepFailure.Carrier]].
 */
case class StepFailure(kind: String, detail: Json = Null) derives RW

object StepFailure {
  /** A throwable that knows its typed failure. */
  trait Carrier {
    def stepFailure: StepFailure
  }

  /** The typed failure `throwable` carries: its own, else the nearest cause's. */
  def of(throwable: Throwable): Option[StepFailure] =
    Iterator.iterate(throwable)(_.getCause).takeWhile(_ != null).take(32).collectFirst {
      case carrier: Carrier => carrier.stepFailure
    }
}
