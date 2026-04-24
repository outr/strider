package strider

import strider.step.Step

/** Contract for workflow template/parent types.
  * Implementations provide the source-of-truth step definitions.
  * When a workflow recycles, it re-reads steps from its parent. */
trait WorkflowParent {
  def workflow: Workflow
  def description: String
  def enabled: Boolean
}
