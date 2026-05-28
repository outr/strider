package strider.step

import fabric.Json
import fabric.rw.*
import lightdb.progress.ProgressManager
import rapid.Task
import strider.Workflow

/**
 * A [[Job]] that supports cooperative pause and resume.
 *
 * On a fresh run, `ctx.resumeFrom` is `None` and `execute` proceeds
 * from the start. On a resume, `ctx.resumeFrom` carries the
 * [[State]] persisted by the prior paused run, and `execute` should
 * seek to that position before continuing.
 *
 * Inside `execute`, call `ctx.checkpoint(currentState)` periodically
 * at safe boundaries (per-record, per-batch). When `checkpoint`
 * returns `true`, the state has been persisted and the job should
 * immediately throw [[JobPausedException]] to exit cleanly; the
 * runner catches the exception, re-queues this step at the head of
 * the queue, and transitions the workflow to
 * [[strider.WorkflowStatus.Paused]] until
 * `manager.unpause(workflowId)` is called.
 *
 * Plain [[Job]]s never see the pause signal — they run to
 * completion. The workflow can still be paused, but the pause only
 * takes effect at the step boundary after the running step
 * finishes. Use [[StatefulJob]] when you need to pause partway
 * through a long step.
 *
 * @tparam Return the return type of the job (serialized to Json as payload)
 * @tparam State  the checkpoint state shape (must have an `RW` for storage)
 */
trait StatefulJob[Return: RW, State: RW] extends Job[Return] {
  /** RW instance for the [[State]] type, used by the runner to
    * serialize and deserialize the checkpoint. */
  def stateRW: RW[State] = summon[RW[State]]

  /** Pausable execute. The job calls `ctx.checkpoint(state)`
    * periodically; when it returns `true`, throw [[JobPausedException]]
    * to exit. */
  def execute(workflow: Workflow,
              pm: ProgressManager,
              ctx: StatefulJobContext[State]): Task[Return]

  /** Non-contextual execute delegates to the contextual one with a
    * no-op [[StatefulJobContext]] so direct callers (specs,
    * standalone invocation) keep working. */
  final override def execute(workflow: Workflow, pm: ProgressManager): Task[Return] =
    execute(workflow, pm, StatefulJobContext.noOp[State])

  /** The runner threads a typed [[StatefulJobContext]] in via the
    * `JobContext` parameter — we cast back to the typed view here.
    * The runner is the only construction site for these contexts,
    * so this cast is safe by construction. Non-runner callers (or
    * non-StatefulJobContext ctxs) fall back to the no-op context,
    * making pause a no-op. */
  final override def executeToJsonContextualized(workflow: Workflow,
                                                  pm: ProgressManager,
                                                  ctx: JobContext): Task[Json] = {
    val sctx: StatefulJobContext[State] = ctx match {
      case s: StatefulJobContext[?] => s.asInstanceOf[StatefulJobContext[State]]
      case _                        => StatefulJobContext.noOp[State]
    }
    execute(workflow, pm, sctx).map(_.json)
  }
}
