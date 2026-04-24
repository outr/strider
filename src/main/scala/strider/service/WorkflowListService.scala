package strider.service

import strider.{AbstractWorkflowManager, AbstractWorkflowModel}
import fabric.rw._
import rapid.Task
import spice.net._
import spice.openapi.server.{OpenAPIHttpServer, RestService}

class WorkflowListService[WorkflowModel <: AbstractWorkflowModel](
  getWorkflowManager: => AbstractWorkflowManager[?, WorkflowModel],
  val server: OpenAPIHttpServer
) extends RestService {
  private lazy val workflowManager: AbstractWorkflowManager[?, WorkflowModel] = getWorkflowManager
  override type Request = WorkflowListRequest
  override type Response = WorkflowListResponse

  private implicit lazy val previewRW: RW[strider.WorkflowPreview] = workflowManager.collection.model.previewRW

  override implicit def requestRW: RW[WorkflowListRequest] = RW.gen
  override implicit def responseRW: RW[WorkflowListResponse] = RW.gen

  override val path: URLPath = path"/workflow/list"

  override protected def summary: String = "List workflows with optional filters"

  override protected def apply(request: WorkflowListRequest): Task[WorkflowListResponse] = workflowManager
    .list(request.status, request.tag, request.name, request.limit)
    .map { workflows =>
      WorkflowListResponse(workflows.map(_.preview))
    }
}
