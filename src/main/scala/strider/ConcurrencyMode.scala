package strider

import fabric.rw.*

/**
 * What a workflow does when it is due to start while another workflow of the same name is running, that is, executing
 * a step in the same manager. A workflow that is scheduled, paused or parked on a trigger or approval is not running, so
 * a recurring template waiting on its trigger never collides with the copies it branches.
 *
 * Only a manager with more than one slot (`maxConcurrentWorkflows > 1`) can start a workflow while another runs; a
 * single-slot manager runs same-named workflows one after another whatever the mode.
 */
sealed trait ConcurrencyMode

object ConcurrencyMode {
  /** The new workflow fails at once, recording the run it collided with. The default. */
  case object Fail extends ConcurrencyMode

  /** The running workflow is cancelled at its next step boundary, and the new one starts once it has stopped. */
  case object CancelAndRestart extends ConcurrencyMode

  /** The new workflow runs alongside the one already running. */
  case object AllowConcurrent extends ConcurrencyMode

  val all: List[ConcurrencyMode] = List(Fail, CancelAndRestart, AllowConcurrent)

  implicit val rw: RW[ConcurrencyMode] = RW.enumeration(all)
}
