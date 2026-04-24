package strider.service

import strider.ErrorMessage
import strider.{AbstractWorkflowManager, AbstractWorkflowModel}
import strider.step.Approval
import fabric.rw._
import rapid.Task
import spice.net._
import spice.openapi.server.{OpenAPIHttpServer, RestService}

class WorkflowStatusService[WorkflowModel <: AbstractWorkflowModel](
  getWorkflowManager: => AbstractWorkflowManager[?, WorkflowModel],
  val server: OpenAPIHttpServer
) extends RestService {
  private lazy val workflowManager: AbstractWorkflowManager[?, WorkflowModel] = getWorkflowManager
  override type Request = WorkflowStatusRequest
  override type Response = WorkflowStatusResponse

  override implicit def requestRW: RW[WorkflowStatusRequest] = WorkflowStatusRequest.rw
  override implicit def responseRW: RW[WorkflowStatusResponse] = WorkflowStatusResponse.rw

  override val path: URLPath = path"/workflow/status"

  override protected def summary: String = "Get the status of a workflow"

  override protected def apply(request: WorkflowStatusRequest): Task[WorkflowStatusResponse] = workflowManager
    .byId(request.workflowId)
    .map {
      case Some(workflow) =>
        val waitingPrompt = for {
          stepId <- workflow.waitingStepId
          step <- workflow.byStepId(stepId)
          approval <- Option(step).collect { case a: Approval => a }
        } yield approval.prompt
        WorkflowStatusResponse(Some(WorkflowInfo(
          id = workflow._id,
          status = workflow.status,
          waitingStepId = workflow.waitingStepId,
          waitingSince = workflow.waitingSince,
          waitingPrompt = waitingPrompt
        )), None)
      case None => WorkflowStatusResponse(None, Some(ErrorMessage("Unable to find workflow by id")))
    }
}
