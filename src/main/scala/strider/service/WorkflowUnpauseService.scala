package strider.service

import strider.ErrorMessage
import strider.{AbstractWorkflowManager, AbstractWorkflowModel}
import fabric.rw._
import rapid.Task
import spice.net._
import spice.openapi.server.{OpenAPIHttpServer, RestService}

/** Resume a paused workflow. The runner picks the workflow back up on its
  * next monitor tick and the paused step's `StatefulJobContext.resumeFrom`
  * surfaces the persisted checkpoint. */
class WorkflowUnpauseService[WorkflowModel <: AbstractWorkflowModel](
  getWorkflowManager: => AbstractWorkflowManager[?, WorkflowModel],
  val server: OpenAPIHttpServer
) extends RestService {
  private lazy val workflowManager: AbstractWorkflowManager[?, WorkflowModel] = getWorkflowManager
  override type Request = WorkflowUnpauseRequest
  override type Response = WorkflowStatusResponse

  override implicit def requestRW: RW[WorkflowUnpauseRequest] = RW.gen
  override implicit def responseRW: RW[WorkflowStatusResponse] = WorkflowStatusResponse.rw

  override val path: URLPath = path"/workflow/unpause"

  override protected def summary: String = "Resume a paused workflow"

  override protected def apply(request: WorkflowUnpauseRequest): Task[WorkflowStatusResponse] = workflowManager
    .unpause(request.workflowId)
    .map { workflow =>
      WorkflowStatusResponse(Some(WorkflowInfo(
        id = workflow._id,
        status = workflow.status
      )), None)
    }
    .handleError { throwable =>
      Task.pure(WorkflowStatusResponse(None, Some(ErrorMessage(throwable.getMessage))))
    }
}
