package strider

import fabric.rw._

enum WorkflowStatus derives RW {
  case Pending, Scheduled, Running, Waiting, Paused, Success, Failure, Cancelled, TimedOut

  lazy val index: Int = WorkflowStatus.values.indexOf(this)
}

object WorkflowStatus {
  implicit val ordering: Ordering[WorkflowStatus] = Ordering.by(_.index)
}
