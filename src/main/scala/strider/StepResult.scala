package strider

import strider.step.*

import fabric.Json
import fabric.rw._
import lightdb.id.Id

/**
 * One execution of a step. A failed step carries its message in `error` and, when the throwable it failed with carries
 * one, its typed `failure` ([[StepFailure]]). A condition step records its decision in `condition`.
 */
case class StepResult(stepId: Id[Step],
                      stepName: String,
                      status: StepResultStatus,
                      output: Option[Json] = None,
                      durationMs: Long = 0L,
                      error: Option[String] = None,
                      failure: Option[StepFailure] = None,
                      condition: Option[ConditionOutcome] = None) derives RW
