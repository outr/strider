package strider

import strider.step.*

import lightdb.time.Timestamp
import lightdb.id.Id

case class WorkflowPreview(name: String,
                           steps: List[Step],
                           scheduled: Long,
                           queue: List[Id[Step]],
                           sourceId: Id[WorkflowParent],
                           priority: Double,
                           runningId: Option[Id[Step]],
                           waitingStepId: Option[Id[Step]],
                           pausedStepId: Option[Id[Step]] = None,
                           pauseRequested: Boolean = false,
                           stepProgress: Option[Double],
                           stepMessage: Option[String],
                           workflowProgress: Option[Double],
                           completed: List[Id[Step]],
                           history: List[WorkflowHistory],
                           created: Timestamp,
                           modified: Timestamp,
                           workflowId: Id[Workflow]) {

  def jobs: List[Job[?]] = steps.collect { case j: Job[?] => j }

  lazy val activity: WorkflowActivity = WorkflowState.activity(history)
  lazy val finished: Boolean = WorkflowState.finished(history)
  lazy val started: Boolean = WorkflowState.started(history)
  lazy val paused: Boolean = pausedStepId.isDefined
  lazy val status: WorkflowStatus = WorkflowState.status(history, waitingStepId, pausedStepId)
}
