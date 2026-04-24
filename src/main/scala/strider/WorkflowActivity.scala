package strider

import strider.step.*

import fabric.Json
import fabric.rw._
import lightdb.id.Id

sealed trait WorkflowActivity

object WorkflowActivity {
  implicit val rw: RW[WorkflowActivity] = RW.poly()(
    RW.static(Created),
    RW.gen[Scheduled],
    RW.static(Starting),
    RW.gen[StepStarted],
    RW.gen[StepSuccess],
    RW.gen[StepFailure],
    RW.gen[StepRetrying],
    RW.gen[WaitingOnTrigger],
    RW.gen[WaitingOnApproval],
    RW.gen[Resumed],
    RW.gen[BranchTaken],
    RW.gen[LoopIteration],
    RW.gen[ChildWorkflowStarted],
    RW.gen[ChildWorkflowCompleted],
    RW.static(Cancelled),
    RW.gen[TimedOut],
    RW.gen[Completed]
  )

  case object Created extends WorkflowActivity

  case class Scheduled(timeStamp: Long) extends WorkflowActivity

  case object Starting extends WorkflowActivity

  // Step lifecycle
  case class StepStarted(stepId: Id[Step]) extends WorkflowActivity
  case class StepSuccess(stepId: Id[Step]) extends WorkflowActivity
  case class StepFailure(stepId: Id[Step], errorMessage: String) extends WorkflowActivity
  case class StepRetrying(stepId: Id[Step], attempt: Int, maxAttempts: Int) extends WorkflowActivity

  // Waiting states
  case class WaitingOnTrigger(stepId: Id[Step]) extends WorkflowActivity
  case class WaitingOnApproval(stepId: Id[Step]) extends WorkflowActivity
  case class Resumed(stepId: Id[Step], payload: Json) extends WorkflowActivity

  // Branching / flow control
  case class BranchTaken(conditionStepId: Id[Step], targetStepId: Id[Step]) extends WorkflowActivity
  case class LoopIteration(stepId: Id[Step], index: Int) extends WorkflowActivity
  case class ChildWorkflowStarted(stepId: Id[Step], childWorkflowId: Id[Workflow]) extends WorkflowActivity
  case class ChildWorkflowCompleted(stepId: Id[Step], childWorkflowId: Id[Workflow], success: Boolean) extends WorkflowActivity

  case object Cancelled extends WorkflowActivity
  case class TimedOut(stepId: Option[Id[Step]]) extends WorkflowActivity

  case class Completed(success: Boolean) extends WorkflowActivity

  // Backward compatibility — JobStarted etc. are just aliases for StepStarted etc.
  @deprecated("Use StepStarted", "2.0")
  type JobStarted = StepStarted
  @deprecated("Use StepStarted", "2.0")
  def JobStarted(jobId: Id[Step]): StepStarted = StepStarted(jobId)
  @deprecated("Use StepSuccess", "2.0")
  type JobSuccess = StepSuccess
  @deprecated("Use StepSuccess", "2.0")
  def JobSuccess(jobId: Id[Step]): StepSuccess = StepSuccess(jobId)
  @deprecated("Use StepFailure", "2.0")
  type JobFailure = StepFailure
  @deprecated("Use StepFailure", "2.0")
  def JobFailure(jobId: Id[Step], errorMessage: String): StepFailure = StepFailure(jobId, errorMessage)
}
