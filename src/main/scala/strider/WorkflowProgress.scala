package strider

import strider.WorkflowProgress.get
import lightdb.id.Id
import reactify.Var

object WorkflowProgress extends Var[Option[ProgressUpdate]](None) {
  def progressFor(workflowId: Id[Workflow]): Option[ProgressUpdate] = get match {
    case Some(p) if p.workflow._id == workflowId => Some(p)
    case _ => None
  }
}
