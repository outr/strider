package strider.service

import strider.Workflow
import fabric.rw._
import lightdb.id.Id

case class WorkflowStatusRequest(workflowId: Id[Workflow])

object WorkflowStatusRequest {
  implicit val rw: RW[WorkflowStatusRequest] = RW.gen
}