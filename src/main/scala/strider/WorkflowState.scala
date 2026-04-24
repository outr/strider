package strider

import strider.step.*

import lightdb.id.Id

/** Shared computation for workflow state derived from history. Used by both Workflow and WorkflowPreview. */
object WorkflowState {
  def activity(history: List[WorkflowHistory]): WorkflowActivity = history.head.activity

  def finished(history: List[WorkflowHistory]): Boolean =
    history.exists(_.activity.isInstanceOf[WorkflowActivity.Completed])

  def started(history: List[WorkflowHistory]): Boolean =
    history.exists(_.activity == WorkflowActivity.Starting)

  def status(history: List[WorkflowHistory], waitingStepId: Option[Id[Step]]): WorkflowStatus = {
    if (finished(history)) {
      history.collectFirst { case h if h.activity.isInstanceOf[WorkflowActivity.Completed] =>
        h.activity.asInstanceOf[WorkflowActivity.Completed]
      } match {
        case Some(c) if c.success => WorkflowStatus.Success
        case _ => WorkflowStatus.Failure
      }
    } else if (history.exists(_.activity.isInstanceOf[WorkflowActivity.Cancelled.type])) {
      WorkflowStatus.Cancelled
    } else if (history.exists(_.activity.isInstanceOf[WorkflowActivity.TimedOut])) {
      WorkflowStatus.TimedOut
    } else if (waitingStepId.isDefined) {
      WorkflowStatus.Waiting
    } else if (started(history)) {
      WorkflowStatus.Running
    } else {
      WorkflowStatus.Scheduled
    }
  }
}
