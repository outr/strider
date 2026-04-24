package strider.service

import strider.ErrorMessage
import strider.{AbstractWorkflowManager, AbstractWorkflowModel}
import fabric.rw._
import rapid.Task
import spice.net._
import spice.openapi.server.{OpenAPIHttpServer, RestService}

class WorkflowResumeService[WorkflowModel <: AbstractWorkflowModel](
  getWorkflowManager: => AbstractWorkflowManager[?, WorkflowModel],
  val server: OpenAPIHttpServer
) extends RestService {
  private lazy val workflowManager: AbstractWorkflowManager[?, WorkflowModel] = getWorkflowManager
  override type Request = WorkflowResumeRequest
  override type Response = WorkflowStatusResponse

  override implicit def requestRW: RW[WorkflowResumeRequest] = RW.gen
  override implicit def responseRW: RW[WorkflowStatusResponse] = WorkflowStatusResponse.rw

  override val path: URLPath = path"/workflow/resume"

  override protected def summary: String = "Resume a waiting workflow"

  override protected def apply(request: WorkflowResumeRequest): Task[WorkflowStatusResponse] = workflowManager
    .resume(request.workflowId, request.stepId, request.payload)
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
