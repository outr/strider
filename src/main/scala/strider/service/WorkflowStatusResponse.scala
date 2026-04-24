package strider.service

import strider.{ErrorMessage, GeneralResponse}
import fabric.rw._

case class WorkflowStatusResponse(info: Option[WorkflowInfo], error: Option[ErrorMessage]) extends GeneralResponse

object WorkflowStatusResponse {
  implicit val rw: RW[WorkflowStatusResponse] = RW.gen
}