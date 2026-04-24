package strider.step

import strider.Workflow

import fabric.Json
import rapid.Task

trait Trigger extends Step {
  def mode: TriggerMode = TriggerMode.Continue
  def timeoutMs: Option[Long] = None
  def timeoutAction: TimeoutAction = TimeoutAction.Fail

  /**
   * Called when the workflow reaches this trigger step. Implementation should register
   * whatever listener, poller, or scheduled check is needed.
   *
   * @return metadata about the registration (stored in payloads)
   */
  def register(workflow: Workflow): Task[Json]

  /**
   * Called periodically by the workflow manager to check if the trigger condition has been met.
   * For event-driven triggers, this may check a flag set by an external callback.
   * For poll-based triggers, this performs the actual check.
   *
   * @return Some(payload) if triggered, None if still waiting
   */
  def check(workflow: Workflow): Task[Option[Json]]

  /**
   * Called when the trigger is being cleaned up (workflow cancelled, timed out, or completed).
   */
  def unregister(workflow: Workflow): Task[Unit] = Task.unit
}
