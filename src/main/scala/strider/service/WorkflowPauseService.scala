package strider.service

import strider.ErrorMessage
import strider.{AbstractWorkflowManager, AbstractWorkflowModel}
import fabric.rw._
import rapid.Task
import spice.net._
import spice.openapi.server.{OpenAPIHttpServer, RestService}

/** Request a cooperative pause on a running workflow. If the running step is
  * a `StatefulJob`, it pauses at its next checkpoint; otherwise pause is
  * deferred until the current step completes. */
class WorkflowPauseService[WorkflowModel <: AbstractWorkflowModel](
  getWorkflowManager: => AbstractWorkflowManager[?, WorkflowModel],
  val server: OpenAPIHttpServer
) extends RestService {
  private lazy val workflowManager: AbstractWorkflowManager[?, WorkflowModel] = getWorkflowManager
  override type Request = WorkflowPauseRequest
  override type Response = WorkflowStatusResponse

  override implicit def requestRW: RW[WorkflowPauseRequest] = RW.gen
  override implicit def responseRW: RW[WorkflowStatusResponse] = WorkflowStatusResponse.rw

  override val path: URLPath = path"/workflow/pause"

  override protected def summary: String = "Request a cooperative pause on a workflow"

  override protected def apply(request: WorkflowPauseRequest): Task[WorkflowStatusResponse] = workflowManager
    .pause(request.workflowId)
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
