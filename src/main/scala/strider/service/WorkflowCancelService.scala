package strider.service

import strider.ErrorMessage
import strider.{AbstractWorkflowManager, AbstractWorkflowModel}
import fabric.rw._
import rapid.Task
import spice.net._
import spice.openapi.server.{OpenAPIHttpServer, RestService}

class WorkflowCancelService[WorkflowModel <: AbstractWorkflowModel](
  getWorkflowManager: => AbstractWorkflowManager[?, WorkflowModel],
  val server: OpenAPIHttpServer
) extends RestService {
  private lazy val workflowManager: AbstractWorkflowManager[?, WorkflowModel] = getWorkflowManager
  override type Request = WorkflowCancelRequest
  override type Response = WorkflowStatusResponse

  override implicit def requestRW: RW[WorkflowCancelRequest] = RW.gen
  override implicit def responseRW: RW[WorkflowStatusResponse] = WorkflowStatusResponse.rw

  override val path: URLPath = path"/workflow/cancel"

  override protected def summary: String = "Cancel a workflow"

  override protected def apply(request: WorkflowCancelRequest): Task[WorkflowStatusResponse] = workflowManager
    .cancel(request.workflowId)
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
