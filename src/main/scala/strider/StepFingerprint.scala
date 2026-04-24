package strider

import strider.step.Step
import fabric.{Json, Obj}
import fabric.rw.*

object StepFingerprint {
  def of(step: Step)(implicit rw: RW[Step]): Json = {
    val json = step.json
    val obj = json.asObj
    Obj(obj.value.filterNot(_._1 == "id"))
  }

  def matches(a: Step, b: Step)(implicit rw: RW[Step]): Boolean =
    of(a) == of(b)
}
