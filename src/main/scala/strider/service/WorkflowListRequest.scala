package strider.service

import strider.WorkflowStatus
import fabric.rw._

case class WorkflowListRequest(status: Option[WorkflowStatus] = None,
                               tag: Option[String] = None,
                               name: Option[String] = None,
                               limit: Int = 50) derives RW
