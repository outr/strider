package strider.step

import strider.Workflow

import fabric.Json
import fabric.rw._
import lightdb.id.Id
import lightdb.progress.ProgressManager
import rapid.Task

trait Job[Return: RW] extends Step {
  def id: Id[Step]

  /**
   * Executes this Job returning the payload upon completion.
   *
   * @param workflow the workflow this job is being executed against
   * @param pm       an optional mechanism to update the progress of this Job's execution. If progress cannot be
   *                 represented as a percentage (0.0 - 1.0), setting a message alone shows indeterminate progress.
   *
   * @return the result value, automatically serialized to Json via the RW context bound
   */
  def execute(workflow: Workflow, pm: ProgressManager): Task[Return]

  /** Serialize the return value to Json for storage in workflow payloads. */
  def executeToJson(workflow: Workflow, pm: ProgressManager): Task[Json] =
    execute(workflow, pm).map(r => r.json)

  /** Variant of [[executeToJson]] that receives the runner's
    * [[JobContext]] — gives the step a way to mutate the running
    * workflow within the runner's own transaction. Steps that need
    * to append next-iteration steps from inside their execute body
    * (agent ReAct loops, dynamic step composition) override this.
    * Default delegates to `executeToJson`, so existing jobs keep
    * working unchanged. */
  def executeToJsonContextualized(workflow: Workflow,
                                  pm: ProgressManager,
                                  ctx: JobContext): Task[Json] =
    executeToJson(workflow, pm)
}

object Job
