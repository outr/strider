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
}

object Step {
  def id(): Id[Step] = Id()
}
