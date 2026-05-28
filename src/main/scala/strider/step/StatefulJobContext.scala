package strider.step

import fabric.Json
import lightdb.id.Id
import rapid.Task
import strider.Workflow

/**
 * Typed execute-time handle for a [[StatefulJob]]. Adds cooperative
 * pause + resume semantics on top of the base [[JobContext]]:
 *
 *  - [[resumeFrom]] yields the state persisted by the prior paused
 *    run of this step, or `None` for a fresh execution. The job
 *    consults it on entry to seek to where it left off.
 *
 *  - [[checkpoint]] is the cooperative pause point. The job calls
 *    it periodically (per-record, per-batch, or per-N-iterations).
 *    When no pause has been requested it returns `false` in O(1)
 *    without evaluating `state` (by-name argument), so it's cheap
 *    to call in hot loops. When pause has been requested it
 *    persists `state.json` to the workflow record, writes a
 *    [[strider.WorkflowActivity.Paused]] activity, and returns
 *    `true`. The caller should then immediately
 *    `throw new JobPausedException` to exit `execute`; the runner
 *    catches it and transitions the workflow to
 *    [[strider.WorkflowStatus.Paused]].
 */
trait StatefulJobContext[State] extends JobContext {
  /** State persisted by the prior paused run of this step, if this
    * is a resume. None on a fresh execution. */
  def resumeFrom: Option[State]

  /** Cooperative pause point. See class docs. Returns `true` when
    * the job should immediately throw [[JobPausedException]] to
    * exit; `false` to keep running. */
  def checkpoint(state: => State): Task[Boolean]
}

object StatefulJobContext {
  /** A no-op context for [[StatefulJob]]s called outside the runner —
    * `resumeFrom` is always `None` and `checkpoint` is always `false`.
    * Used by [[StatefulJob.execute(Workflow, ProgressManager)]] so
    * direct (non-managed) invocation keeps working. */
  def noOp[State]: StatefulJobContext[State] = new StatefulJobContext[State] {
    override def resumeFrom: Option[State] = None
    override def checkpoint(state: => State): Task[Boolean] = Task.pure(false)
    override def updateStepsInTxn(workflowId: Id[Workflow], newSteps: List[Step]): Task[Workflow] =
      Task.error(new UnsupportedOperationException("noOp StatefulJobContext has no transaction"))
  }
}
