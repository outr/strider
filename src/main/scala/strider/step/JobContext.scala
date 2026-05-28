package strider.step

import lightdb.id.Id
import rapid.Task
import strider.Workflow

/**
 * Per-execute handle that exposes the workflow runner's transaction
 * to a [[Job]] so step bodies can mutate the running workflow's
 * step list / queue from inside their own `execute` and have the
 * change observed by the runner's NEXT step pop.
 *
 * Without this, calling `manager.updateSteps` from inside a job's
 * execute uses a separate transaction whose writes get clobbered
 * by the runner's post-execute `modify` (which re-reads the
 * workflow row from its own txn snapshot — pre-updateSteps —
 * and writes the runner's incremental changes against that stale
 * view).
 *
 * Default-impl jobs don't need this — `executeToJson` is unchanged.
 * Steps that DO need to append work mid-execute (agent ReAct loops,
 * dynamic step composition) override
 * [[Job.executeToJsonContextualized]].
 */
trait JobContext {
  /** Mutate the running workflow's step list using the runner's
    * current transaction. Use this to append next-iteration steps
    * from inside a job's execute — the new queue is observed by
    * the runner's next pop. */
  def updateStepsInTxn(workflowId: Id[Workflow], newSteps: List[Step]): Task[Workflow]
}

/** Thrown by a [[StatefulJob]] from inside `execute` after
  * `ctx.checkpoint(state)` returns `true`. The runner catches this
  * exception specifically and treats it as a cooperative pause — the
  * step is re-queued at the head of the queue and the workflow
  * transitions to [[strider.WorkflowStatus.Paused]] until
  * `manager.unpause(workflowId)` is called.
  *
  * The checkpoint state is already persisted by the time this is
  * thrown; the exception carries no payload itself. */
class JobPausedException extends RuntimeException("Step paused cooperatively") {
  // Stack trace is irrelevant for control flow — skip it.
  override def fillInStackTrace(): Throwable = this
}
