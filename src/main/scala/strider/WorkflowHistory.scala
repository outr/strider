package strider

import fabric.rw._

case class WorkflowHistory(activity: WorkflowActivity, created: Long = System.currentTimeMillis())

object WorkflowHistory {
  implicit val rw: RW[WorkflowHistory] = RW.gen
}