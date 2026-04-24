package strider.step

import fabric.rw._

enum RetryBackoff derives RW {
  case Fixed
  case Exponential
  case Linear

  def delayForAttempt(baseDelayMs: Long, attempt: Int): Long = this match {
    case Fixed => baseDelayMs
    case Exponential => baseDelayMs * Math.pow(2, attempt).toLong
    case Linear => baseDelayMs * (attempt + 1)
  }
}
