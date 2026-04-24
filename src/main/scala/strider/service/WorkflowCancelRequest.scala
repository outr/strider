package strider.service

import strider.Workflow
import fabric.rw._
import lightdb.id.Id

case class WorkflowCancelRequest(workflowId: Id[Workflow]) derives RW
