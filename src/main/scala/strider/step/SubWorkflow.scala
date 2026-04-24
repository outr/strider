package strider.step

import fabric.Json
import fabric.rw._
import lightdb.id.Id

case class SubWorkflow(childName: String,
                       childStepsJson: Json,
                       inputVariables: Map[String, Json] = Map.empty,
                       id: Id[Step] = Step.id()) extends Step {
  /** Resolve child steps using the provided step RW. */
  def childSteps(implicit stepRW: RW[Step]): List[Step] = {
    childStepsJson.as[List[Step]]
  }
}

object SubWorkflow {
  def apply(childName: String, childSteps: List[Step], inputVariables: Map[String, Json])(implicit stepRW: RW[Step]): SubWorkflow = {
    SubWorkflow(childName, childSteps.json, inputVariables)
  }

  implicit val rw: RW[SubWorkflow] = RW.gen
}
