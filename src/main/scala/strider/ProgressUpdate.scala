package strider

import strider.step.*

case class ProgressUpdate(workflow: Workflow,
                          step: Step,
                          stepProgress: Option[Double],
                          workflowProgress: Double,
                          message: Option[String],
                          timeStamp: Long = System.currentTimeMillis()) {
  def job: Option[Job[?]] = step match {
    case j: Job[?] => Some(j)
    case _ => None
  }
}
