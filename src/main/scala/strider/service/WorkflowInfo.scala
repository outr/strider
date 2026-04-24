package strider.service

import strider.step.Step
import strider.{ Workflow, WorkflowStatus}
import fabric.rw._
import lightdb.id.Id

case class WorkflowInfo(id: Id[Workflow],
                        status: WorkflowStatus,
                        waitingStepId: Option[Id[Step]] = None,
                        waitingSince: Option[Long] = None,
                        waitingPrompt: Option[String] = None) derives RW
