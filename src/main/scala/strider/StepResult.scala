package strider

import strider.step.*

import fabric.Json
import fabric.rw._
import lightdb.id.Id

case class StepResult(stepId: Id[Step],
                      stepName: String,
                      status: StepResultStatus,
                      output: Option[Json] = None,
                      durationMs: Long = 0L,
                      error: Option[String] = None) derives RW
