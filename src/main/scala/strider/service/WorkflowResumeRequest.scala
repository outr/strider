package strider.service

import strider.step.Step
import strider.{ Workflow}
import fabric.Json
import fabric.rw._
import lightdb.id.Id

case class WorkflowResumeRequest(workflowId: Id[Workflow],
                                 stepId: Id[Step],
                                 payload: Json) derives RW
