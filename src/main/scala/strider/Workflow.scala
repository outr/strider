package strider

import strider.step.*

import fabric.Json
import lightdb.time.Timestamp
import lightdb.id.Id
import lightdb.doc.RecordDocument

case class Workflow(name: String,
                    steps: List[Step],
                    scheduled: Long,
                    queue: List[Id[Step]],
                    sourceId: Id[WorkflowParent],
                    variableDefs: List[WorkflowVariable] = Nil,
                    variables: Map[String, Json] = Map.empty,
                    tags: Set[String] = Set.empty,
                    priority: Double = 1.0,
                    workflowTimeoutMs: Option[Long] = None,
                    runningId: Option[Id[Step]] = None,
                    waitingStepId: Option[Id[Step]] = None,
                    waitingSince: Option[Long] = None,
                    completed: List[Id[Step]] = Nil,
                    payloads: Map[Id[Step], Json] = Map.empty,
                    stepResults: List[StepResult] = Nil,
                    history: List[WorkflowHistory] = Nil,
                    parentRunId: Option[Id[Workflow]] = None,
                    space: Option[String] = None,
                    createdBy: Option[String] = None,
                    conversationId: Option[String] = None,
                    created: Timestamp = Timestamp(),
                    modified: Timestamp = Timestamp(),
                    _id: Id[Workflow] = Id()) extends RecordDocument[Workflow] {

  def jobs: List[Job[?]] = steps.collect { case j: Job[?] => j }

  def byStepName(name: String): List[Step] = steps.filter(_.name == name)
  def payloadsByStepName(name: String): List[Json] = byStepName(name).flatMap(step => payloads.get(step.id))
  def byStepId(id: Id[Step]): Option[Step] = steps.find(_.id == id)

  @deprecated("Use byStepName", "2.0")
  def byJobName(name: String): List[Job[?]] = jobs.filter(_.name == name)
  @deprecated("Use payloadsByStepName", "2.0")
  def payloadsByName(name: String): List[Json] = byJobName(name).flatMap(job => payloads.get(job.id))
  @deprecated("Use byStepId", "2.0")
  def byId(id: Id[Step]): Option[Step] = byStepId(id)

  def preview: WorkflowPreview = {
    val progress = WorkflowProgress.progressFor(_id)
    WorkflowPreview(
      name = name,
      steps = steps,
      scheduled = scheduled,
      queue = queue,
      sourceId = sourceId,
      priority = priority,
      runningId = runningId,
      waitingStepId = waitingStepId,
      stepProgress = progress.flatMap(_.stepProgress),
      stepMessage = progress.flatMap(_.message),
      workflowProgress = progress.map(_.workflowProgress),
      completed = completed,
      history = history,
      created = created,
      modified = modified,
      workflowId = _id
    )
  }

  def withVariableDefaults: Workflow = {
    val defaults = variableDefs.flatMap { vd =>
      vd.defaultValue.filterNot(_ => variables.contains(vd.name)).map(vd.name -> _)
    }.toMap
    if (defaults.isEmpty) this else copy(variables = defaults ++ variables)
  }

  def validateVariables: List[String] = {
    variableDefs.filter(_.required).map(_.name).filterNot(variables.contains)
  }

  lazy val activity: WorkflowActivity = WorkflowState.activity(history)
  lazy val running: Option[Step] = runningId.flatMap(byStepId)
  lazy val payload: Option[Json] = completed.headOption.flatMap(payloads.get)
  lazy val finished: Boolean = WorkflowState.finished(history)
  lazy val started: Boolean = WorkflowState.started(history)
  lazy val status: WorkflowStatus = WorkflowState.status(history, waitingStepId)
}
