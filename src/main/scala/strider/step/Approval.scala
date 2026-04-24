package strider.step

import strider.Workflow

import rapid.Task

trait Approval extends Step {
  def prompt: String
  def options: List[String] = List("Approve", "Reject")
  def timeoutMs: Option[Long] = None
  def timeoutAction: TimeoutAction = TimeoutAction.Fail

  /**
   * Called when the workflow enters approval wait state.
   * Implementation can send notifications, update UI, etc.
   */
  def onWaiting(workflow: Workflow): Task[Unit] = Task.unit
}
