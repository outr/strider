package strider.service

import strider.{ErrorMessage, GeneralResponse}
import strider.WorkflowPreview
import fabric.rw._

case class WorkflowListResponse(workflows: List[WorkflowPreview],
                                error: Option[ErrorMessage] = None) extends GeneralResponse
