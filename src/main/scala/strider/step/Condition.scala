package strider.step

import strider.Workflow

import lightdb.id.Id
import rapid.Task

/**
 * Chooses one of two steps. The chosen step runs next, once; the other is taken out of the queue and does not run.
 * Every other queued step runs in order after it.
 */
trait Condition extends Step {
  def onTrue: Id[Step]
  def onFalse: Id[Step]

  /**
   * Evaluate the condition using workflow state (variables, payloads, history).
   *
   * @return true to follow onTrue branch, false for onFalse branch
   */
  def evaluate(workflow: Workflow): Task[Boolean]
}
