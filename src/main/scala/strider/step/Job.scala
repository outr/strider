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
}

object Job
