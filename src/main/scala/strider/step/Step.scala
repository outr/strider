package strider.step

import lightdb.id.Id

trait Step {
  def name: String = getClass.getSimpleName.replace("$", "")
  def id: Id[Step]
  def weight: Double = 1.0

  def continueOnError: Boolean = false
  def retryCount: Int = 0
  def retryDelayMs: Long = 5000L
  def retryBackoff: RetryBackoff = RetryBackoff.Fixed

  /** Optional timeout for this step's execution in milliseconds. None means no timeout. */
  def executionTimeoutMs: Option[Long] = None

  /** Named resources this step uses heavily, such as a disk or a rate-limited API. A manager with a limit for a resource
    * runs at most that many top-level job steps holding it at once, across all its workflows; a step waits for its
    * permits before it starts. Steps inside a Parallel or Loop body do not take permits. */
  def resources: Set[String] = Set.empty
}

object Step {
  def id(): Id[Step] = Id()
}
