package spec

import strider.{AbstractWorkflowManager, AbstractWorkflowModel, ProgressUpdate, StepResultStatus, Workflow, WorkflowActivity, WorkflowParent, WorkflowProgress, WorkflowStatus, WorkflowVariable, VariableType}
import strider.step.{Approval, Condition, JoinMode, Job, Loop, Parallel, Recycle, RetryBackoff, Step, SubWorkflow, TimeoutAction, Trigger, TriggerMode}
import fabric.*
import fabric.rw.*
import lightdb.LightDB
import lightdb.filter.FilterExtras
import lightdb.rocksdb.RocksDBStore
import lightdb.id.Id
import lightdb.lucene.LuceneStore
import lightdb.progress.{Progress, ProgressManager}
import lightdb.sql.SQLiteStore
import lightdb.store.{Collection, StoreManager}
import lightdb.store.split.SplitStoreManager
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.monitor.SwingTaskMonitor
import rapid.*
import reactify.Var
import scribe.{Level, Logger}

import java.nio.file.Path
import scala.concurrent.duration.{DurationLong, FiniteDuration}

class WorkflowSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {
  private var workflow1Id: Id[Workflow] = _
  private var workflow2Id: Id[Workflow] = _
  private var workflow3Id: Id[Workflow] = _
  private var workflow4Id: Id[Workflow] = _
  private var workflow5Id: Id[Workflow] = _
  private var workflow6Id: Id[Workflow] = _

  private var progressEvents = List.empty[ProgressUpdate]

  "Workflow" should {
    "initialize database" in {
      db.init.succeed
    }
    "truncate the collection" in {
      db.workflows.transaction(_.truncate.map(_ => succeed))
    }
    "initialize manager" in {
      WorkflowProgress.attach {
        case Some(p) => progressEvents = p :: progressEvents
        case None => // Ignore
      }
      WorkflowManager.init().map(_ => succeed)
    }
//    "hide error logging of failed jobs" in {
//      Logger("strider.AbstractWorkflowManager").withMinimumLevel(Level.Fatal).replace()
//      succeed
//    }
    "schedule a single task workflow" in {
      WorkflowManager.schedule(
        name = "W1",
        steps = List(ReverseTextJob(Right("Hello, World!"))),
        sourceId = testSourceId
      ).map { workflow =>
        workflow1Id = workflow._id
        workflow.jobs.length should be(1)
      }
    }
    "wait for the workflow to finish successfully" in {
      WorkflowManager.waitForFinished(workflow1Id).map { workflow =>
        workflow.jobs.map(_.name) should be(List("ReverseTextJob"))
        workflow.history.map(_.activity.getClass.getSimpleName) should be(List("Completed", "StepSuccess", "StepStarted", "Starting$", "Scheduled", "Created$"))
        workflow.queue.length should be(0)
        workflow.runningId should be(None)
        workflow.completed.length should be(1)
        workflow.payloadsByName("ReverseTextJob") should be(List(str("!dlroW ,olleH")))
        workflow.finished should be(true)
      }
    }
    "schedule a workflow with two reverse tasks" in {
      val job1 = ReverseTextJob(Right("Hello, World!"))
      val job2 = ReverseTextJob(Left(job1.id))
      WorkflowManager.schedule(name = "W2", steps = List(job1, job2), sourceId = testSourceId).map { workflow =>
        workflow2Id = workflow._id
        workflow.jobs.length should be(2)
      }
    }
    "wait for both tasks to complete successfully" in {
      WorkflowManager.waitForFinished(workflow2Id).map { workflow =>
        workflow.jobs.map(_.name) should be(List("ReverseTextJob", "ReverseTextJob"))
        workflow.queue.length should be(0)
        workflow.runningId should be(None)
        workflow.completed.length should be(2)
        workflow.jobs.map(_.id).map(workflow.payloads) should be(List(
          str("!dlroW ,olleH"), str("Hello, World!")
        ))
        workflow.payload should be(Some(str("Hello, World!")))
        workflow.finished should be(true)
        workflow.history.map(_.activity.getClass.getSimpleName) should be(List("Completed", "StepSuccess", "StepStarted", "StepSuccess", "StepStarted", "Starting$", "Scheduled", "Created$"))
      }
    }
    "schedule a workflow with two tasks and the first fails" in {
      WorkflowManager.schedule(
        name = "W3",
        steps = List(FailingJob(), ReverseTextJob(Right("Shouldn't run!"))),
        sourceId = testSourceId
      ).map { workflow =>
        workflow3Id = workflow._id
        workflow.jobs.length should be(2)
      }
    }
    "wait for the workflow to fail" in {
      WorkflowManager.waitForFinished(workflow3Id).map { workflow =>
        workflow.jobs.map(_.name) should be(List("FailingJob", "ReverseTextJob"))
        workflow.queue.length should be(1)
        workflow.runningId should be(None)
        workflow.completed.length should be(1)
        workflow.payloads should be(Map.empty)
        workflow.finished should be(true)
        workflow.history.map(_.activity.getClass.getSimpleName) should be(List("Completed", "StepFailure", "StepStarted", "Starting$", "Scheduled", "Created$"))
      }
    }
    "verify failed workflow status" in {
      WorkflowManager.byId(workflow3Id).map {
        case Some(workflow) =>
          workflow.status should be(WorkflowStatus.Failure)
          workflow.started should be(true)
        case None => fail("Workflow not found")
      }
    }
    "monitor progress states effectively" in {
      progressEvents = Nil
      WorkflowManager.schedule(name = "W4", steps = List(
        TimedJob(250.millis), TimedJob(250.millis, weight = 2.0), TimedJob(250.millis)
      ), sourceId = testSourceId).map { workflow =>
        workflow4Id = workflow._id
        workflow.jobs.length should be(3)
      }
    }
    "wait for the workflow progress to complete" in {
      WorkflowManager.waitForFinished(workflow4Id).map { workflow =>
        workflow.finished should be(true)
        workflow.status should be(WorkflowStatus.Success)
        val progress = progressEvents.map(_.workflowProgress)
        // Progress should start at 0.0 and end at 1.0
        progress.last should be(0.0)
        progress.head should be(1.0)
        // Progress should be monotonically non-decreasing (events are prepended, so list is reversed)
        progress.reverse.sliding(2).foreach {
          case List(a, b) => a should be <= b
          case _ => // single element, ok
        }
        // Should have multiple progress events
        progress.length should be > 3
        // Verify weighted boundary values are present:
        // Jobs have weights 1.0, 2.0, 1.0 (total 4.0)
        // After job1: 1.0/4.0 = 0.25, after job2: 3.0/4.0 = 0.75, after job3: 4.0/4.0 = 1.0
        val distinctProgress = progress.distinct
        distinctProgress should contain(0.25)
        distinctProgress should contain(0.75)
        distinctProgress should contain(1.0)
      }
    }
    "verify per-job progress percentage within a weighted workflow" in {
      progressEvents = Nil
      val job1 = TimedJob(500.millis, weight = 1.0)
      val job2 = TimedJob(500.millis, weight = 3.0)
      for {
        w <- WorkflowManager.schedule(name = "WeightedProgress", steps = List(job1, job2), sourceId = testSourceId)
        finished <- WorkflowManager.waitForFinished(w._id)
      } yield {
        finished.status should be(WorkflowStatus.Success)
        // Total weight = 4.0. Job1 = 1.0, Job2 = 3.0
        // During job1: max progress = 1.0/4.0 = 0.25
        // During job2: starts at 0.25, max = 4.0/4.0 = 1.0
        val job1Events = progressEvents.filter(_.step.id == job1.id)
        val job2Events = progressEvents.filter(_.step.id == job2.id)
        // Job1 progress should never exceed 0.25 (its weight / total)
        job1Events.foreach { e =>
          e.workflowProgress should be <= 0.25
        }
        // Job2 progress should start at or above 0.25 and reach 1.0
        job2Events.foreach { e =>
          e.workflowProgress should be >= 0.25
        }
        // Final event should be 1.0
        progressEvents.head.workflowProgress should be(1.0)
      }
    }
    "schedule a job in the future" in {
      WorkflowManager.scheduleIn(name = "W5", steps = List(ReverseTextJob(Right("!dlroW ,olleH"))), sourceId = testSourceId, duration = 2.seconds).map { workflow =>
        workflow5Id = workflow._id
      }.succeed
    }
    "wait for the delayed job to finish" in {
      val start = System.currentTimeMillis()
      WorkflowManager.waitForFinished(workflow5Id).map { workflow =>
        workflow.finished should be(true)
        workflow.status should be(WorkflowStatus.Success)
        workflow.payload should be(Some(str("Hello, World!")))
        val now = System.currentTimeMillis()
        now - start should be >= 2_000L
      }
    }
    "verify byJobName and payloadsByName lookups" in {
      WorkflowManager.byId(workflow2Id).map {
        case Some(workflow) =>
          workflow.byJobName("ReverseTextJob").length should be(2)
          workflow.byJobName("NonExistent") should be(Nil)
          workflow.payloadsByName("ReverseTextJob").length should be(2)
          workflow.payloadsByName("NonExistent") should be(Nil)
        case None => fail("Workflow not found")
      }
    }
    "verify byId lookup on workflow" in {
      WorkflowManager.byId(workflow1Id).map {
        case Some(workflow) =>
          val job = workflow.jobs.head
          workflow.byId(job.id) should be(Some(job))
          workflow.byId(Step.id()) should be(None)
        case None => fail("Workflow not found")
      }
    }
    "verify WorkflowStatus ordering" in {
      import scala.math.Ordered.orderingToOrdered
      (WorkflowStatus.Scheduled: WorkflowStatus) < (WorkflowStatus.Running: WorkflowStatus) should be(true)
      (WorkflowStatus.Running: WorkflowStatus) < (WorkflowStatus.Success: WorkflowStatus) should be(true)
      (WorkflowStatus.Success: WorkflowStatus) < (WorkflowStatus.Failure: WorkflowStatus) should be(true)
      (WorkflowStatus.Scheduled: WorkflowStatus) < (WorkflowStatus.Failure: WorkflowStatus) should be(true)
      succeed
    }
    "verify successful workflow query methods" in {
      WorkflowManager.byId(workflow1Id).map {
        case Some(workflow) =>
          workflow.status should be(WorkflowStatus.Success)
          workflow.finished should be(true)
          workflow.started should be(true)
          workflow.running should be(None)
          workflow.payload should be(Some(str("!dlroW ,olleH")))
        case None => fail("Workflow not found")
      }
    }
    "verify waitForStatus with allowForLaterStatuses" in {
      // Workflow1 is already Success, waiting for Running with allowForLaterStatuses should return immediately
      WorkflowManager.waitForStatus(workflow1Id, WorkflowStatus.Running, allowForLaterStatuses = true).map { workflow =>
        workflow.status should be(WorkflowStatus.Success)
      }
    }
    "schedule two jobs with different priorities and verify they come back in the proper order" in {
      progressEvents = Nil
      val futureTime = System.currentTimeMillis() + 100L
      WorkflowManager.paused @= true
      for {
        w1 <- WorkflowManager.schedule("P1", List(TimedJob(500.millis)), sourceId = testSourceId, timeStamp = futureTime)
        w2 <- WorkflowManager.schedule("P2", List(TimedJob(500.millis)), sourceId = testSourceId, timeStamp = futureTime, priority = 2.0)
        _ = WorkflowManager.paused @= false
        w2Finished <- WorkflowManager.waitForFinished(w2._id)
        w1Status <- WorkflowManager.waitForStatus(w1._id, WorkflowStatus.Running)
//        w1Status <- WorkflowManager.byId(w1._id).map(_.get)
        w1Finished <- WorkflowManager.waitForFinished(w1._id)
      } yield {
        w2Finished.status should be(WorkflowStatus.Success)
        w1Status.status should be(WorkflowStatus.Running)
        w1Finished.status should be(WorkflowStatus.Success)
      }
    }
    "verify pause prevents execution and resume restarts it" in {
      WorkflowManager.paused @= true
      // Schedule in the future to prevent immediate execution even if paused flag races
      val futureTime = System.currentTimeMillis() + 60_000L
      for {
        w <- WorkflowManager.schedule("PauseTest", steps = List(ReverseTextJob(Right("paused"))), sourceId = testSourceId, timeStamp = futureTime)
        _ <- Task.sleep(500.millis)
        beforeResume <- WorkflowManager.byId(w._id).map(_.get)
        // Reschedule to now and unpause
        _ <- db.workflows.transaction { txn =>
          txn.modify(w._id) {
            case Some(wf) => Task.pure(Some(wf.copy(scheduled = System.currentTimeMillis())))
            case None => Task.pure(None)
          }.flatMap(_ => txn.commit)
        }
        _ = WorkflowManager.paused @= false
        finished <- WorkflowManager.waitForFinished(w._id)
      } yield {
        beforeResume.finished should be(false)
        beforeResume.status should be(WorkflowStatus.Scheduled)
        finished.status should be(WorkflowStatus.Success)
        finished.payload should be(Some(str("desuap")))
      }
    }
    "verify same priority workflows execute in scheduled order" in {
      WorkflowManager.paused @= true
      val time = System.currentTimeMillis()
      for {
        w1 <- WorkflowManager.schedule("SamePri1", List(ReverseTextJob(Right("first"))), sourceId = testSourceId, timeStamp = time)
        w2 <- WorkflowManager.schedule("SamePri2", List(ReverseTextJob(Right("second"))), sourceId = testSourceId, timeStamp = time + 1)
        _ = WorkflowManager.paused @= false
        f1 <- WorkflowManager.waitForFinished(w1._id)
        f2 <- WorkflowManager.waitForFinished(w2._id)
      } yield {
        // Both should succeed; w1 should complete first since it was scheduled first with same priority
        f1.status should be(WorkflowStatus.Success)
        f2.status should be(WorkflowStatus.Success)
        // w1 completed timestamp should be before w2
        f1.modified should be < f2.modified
      }
    }
    // Phase 7: New feature tests

    "retry a failing job that succeeds on third attempt" in {
      for {
        w <- WorkflowManager.schedule("RetryTest", steps = List(
          RetryableJob(failTimes = 2, id = Step.id())
        ), sourceId = testSourceId)
        finished <- WorkflowManager.waitForFinished(w._id)
      } yield {
        finished.status should be(WorkflowStatus.Success)
        finished.payload should be(Some(str("success after 3 attempts")))
        // Should have retry history entries
        val retries = finished.history.collect { case h if h.activity.isInstanceOf[WorkflowActivity.StepRetrying] => h }
        retries.length should be(2)
      }
    }
    "continue on error when continueOnError is true" in {
      val failJob = ContinueOnErrorJob()
      val nextJob = ReverseTextJob(Right("after-error"))
      for {
        w <- WorkflowManager.schedule("ContinueOnError", steps = List(failJob, nextJob), sourceId = testSourceId)
        finished <- WorkflowManager.waitForFinished(w._id)
      } yield {
        finished.status should be(WorkflowStatus.Success)
        // Both steps should be completed
        finished.completed.length should be(2)
        // The second job should have run and produced output
        finished.payloads(nextJob.id) should be(str("rorre-retfa"))
        // The failing job should have a StepFailure in history but NOT Completed(false)
        val failures = finished.history.collect { case h if h.activity.isInstanceOf[WorkflowActivity.StepFailure] => h }
        failures.length should be(1)
      }
    }
    "enter waiting state on trigger and resume externally" in {
      val trigger = TestTrigger()
      val afterJob = ReverseTextJob(Right("after-trigger"))
      for {
        w <- WorkflowManager.schedule("TriggerTest", steps = List(trigger, afterJob), sourceId = testSourceId)
        _ <- WorkflowManager.waitForStatus(w._id, WorkflowStatus.Waiting)
        waiting <- WorkflowManager.byId(w._id).map(_.get)
        _ = waiting.status should be(WorkflowStatus.Waiting)
        _ = waiting.waitingStepId should be(Some(trigger.id))
        resumed <- WorkflowManager.resume(w._id, trigger.id, str("triggered!"))
        finished <- WorkflowManager.waitForFinished(w._id)
      } yield {
        finished.status should be(WorkflowStatus.Success)
        finished.payloads(trigger.id) should be(str("triggered!"))
        finished.payloads(afterJob.id) should be(str("reggirt-retfa"))
      }
    }
    "evaluate condition and branch to onTrue" in {
      val trueJob = ReverseTextJob(Right("true-branch"))
      val falseJob = ReverseTextJob(Right("false-branch"))
      val condition = TestCondition(result = true, onTrue = trueJob.id, onFalse = falseJob.id)
      for {
        w <- WorkflowManager.schedule("ConditionTrue", steps = List(condition, trueJob, falseJob), sourceId = testSourceId)
        finished <- WorkflowManager.waitForFinished(w._id)
      } yield {
        finished.status should be(WorkflowStatus.Success)
        // True branch should have executed
        finished.payloads.contains(trueJob.id) should be(true)
        finished.payloads(trueJob.id) should be(str("hcnarb-eurt"))
        // Condition should have BranchTaken in history
        val branches = finished.history.collect { case h if h.activity.isInstanceOf[WorkflowActivity.BranchTaken] => h.activity.asInstanceOf[WorkflowActivity.BranchTaken] }
        branches.length should be(1)
        branches.head.targetStepId should be(trueJob.id)
      }
    }
    "evaluate condition and branch to onFalse" in {
      val trueJob = ReverseTextJob(Right("true-branch"))
      val falseJob = ReverseTextJob(Right("false-branch"))
      val condition = TestCondition(result = false, onTrue = trueJob.id, onFalse = falseJob.id)
      for {
        w <- WorkflowManager.schedule("ConditionFalse", steps = List(condition, trueJob, falseJob), sourceId = testSourceId)
        finished <- WorkflowManager.waitForFinished(w._id)
      } yield {
        finished.status should be(WorkflowStatus.Success)
        finished.payloads.contains(falseJob.id) should be(true)
        finished.payloads(falseJob.id) should be(str("hcnarb-eslaf"))
      }
    }
    "enter approval wait and approve" in {
      val approval = TestApproval()
      val afterJob = ReverseTextJob(Right("approved"))
      for {
        w <- WorkflowManager.schedule("ApprovalTest", steps = List(approval, afterJob), sourceId = testSourceId)
        _ <- WorkflowManager.waitForStatus(w._id, WorkflowStatus.Waiting)
        waiting <- WorkflowManager.byId(w._id).map(_.get)
        _ = waiting.waitingStepId should be(Some(approval.id))
        _ <- WorkflowManager.resume(w._id, approval.id, str("Approve"))
        finished <- WorkflowManager.waitForFinished(w._id)
      } yield {
        finished.status should be(WorkflowStatus.Success)
        finished.payloads(approval.id) should be(str("Approve"))
        finished.payloads(afterJob.id) should be(str("devorppa"))
      }
    }
    "timeout a trigger with TimeoutAction.Fail" in {
      val trigger = TestTrigger(timeoutOverride = Some(500L), timeoutActionOverride = TimeoutAction.Fail)
      for {
        w <- WorkflowManager.schedule("TimeoutFail", steps = List(trigger), sourceId = testSourceId)
        _ <- WorkflowManager.waitForStatus(w._id, WorkflowStatus.Waiting)
        finished <- WorkflowManager.waitForFinished(w._id)
      } yield {
        finished.status should be(WorkflowStatus.Failure)
        val timedOut = finished.history.exists(_.activity.isInstanceOf[WorkflowActivity.TimedOut])
        timedOut should be(true)
      }
    }
    "timeout a trigger with TimeoutAction.Proceed" in {
      val trigger = TestTrigger(timeoutOverride = Some(500L), timeoutActionOverride = TimeoutAction.Proceed)
      val afterJob = ReverseTextJob(Right("after-timeout"))
      for {
        w <- WorkflowManager.schedule("TimeoutProceed", steps = List(trigger, afterJob), sourceId = testSourceId)
        _ <- WorkflowManager.waitForStatus(w._id, WorkflowStatus.Waiting)
        finished <- WorkflowManager.waitForFinished(w._id)
      } yield {
        finished.status should be(WorkflowStatus.Success)
        finished.payloads(afterJob.id) should be(str("tuoemit-retfa"))
      }
    }
    "cancel a waiting workflow" in {
      val trigger = TestTrigger()
      for {
        w <- WorkflowManager.schedule("CancelTest", steps = List(trigger, ReverseTextJob(Right("never"))), sourceId = testSourceId)
        _ <- WorkflowManager.waitForStatus(w._id, WorkflowStatus.Waiting)
        cancelled <- WorkflowManager.cancel(w._id)
      } yield {
        cancelled.status should be(WorkflowStatus.Failure)
        cancelled.finished should be(true)
        val hasCancelled = cancelled.history.exists(_.activity == WorkflowActivity.Cancelled)
        hasCancelled should be(true)
      }
    }
    "branch a new clone from a Branch trigger and leave original waiting" in {
      val setupJob = ReverseTextJob(Right("setup-data"))
      val trigger = BranchTrigger()
      val afterJob = ReverseTextJob(Right("after-trigger"))
      for {
        w <- WorkflowManager.schedule("BranchTest", steps = List(setupJob, trigger, afterJob), sourceId = testSourceId)
        _ <- WorkflowManager.waitForStatus(w._id, WorkflowStatus.Waiting)
        original <- WorkflowManager.byId(w._id).map(_.get)
        // Fire the trigger — should create a clone, original stays parked
        _ <- WorkflowManager.resume(w._id, trigger.id, str("branch-payload-1"))
        _ <- Task.sleep(2.seconds)
        stillWaiting <- WorkflowManager.byId(w._id).map(_.get)
        clones <- db.workflows.transaction(_.query.filter(_.parentRunId === Some(w._id)).stream.toList)
        // Fire again — second clone
        _ <- WorkflowManager.resume(w._id, trigger.id, str("branch-payload-2"))
        _ <- Task.sleep(2.seconds)
        allClones <- db.workflows.transaction(_.query.filter(_.parentRunId === Some(w._id)).stream.toList)
      } yield {
        // Original should have setup payload and still be waiting
        original.waitingStepId should be(Some(trigger.id))
        original.payloads(setupJob.id) should be(str("atad-putes"))
        // Original stays parked at trigger
        stillWaiting.waitingStepId should be(Some(trigger.id))
        stillWaiting.finished should be(false)
        // First clone completed with all payloads
        clones.length should be(1)
        val clone1 = clones.head
        clone1.finished should be(true)
        clone1.status should be(WorkflowStatus.Success)
        clone1.payloads(setupJob.id) should be(str("atad-putes"))
        clone1.payloads(trigger.id) should be(str("branch-payload-1"))
        clone1.payloads(afterJob.id) should be(str("reggirt-retfa"))
        // Two clones total after second firing
        allClones.length should be(2)
        allClones.head.finished should be(true)
        allClones.last.finished should be(true)
      }
    }
    "recycle a workflow after completion to create a fresh trigger-ready copy" in {
      val trigger = TestTrigger()
      val job = ReverseTextJob(Right("recycle-me"))
      val recycle = Recycle()
      for {
        w <- WorkflowManager.schedule("RecycleTest", steps = List(trigger, job, recycle), sourceId = testSourceId)
        _ <- WorkflowManager.waitForStatus(w._id, WorkflowStatus.Waiting)
        // Resume the trigger — workflow should run job, then recycle
        _ <- WorkflowManager.resume(w._id, trigger.id, str("go"))
        finished <- WorkflowManager.waitForFinished(w._id)
        // The recycled fresh copy should exist and be waiting at the trigger
        _ <- Task.sleep(1.second)
        all <- db.workflows.transaction(_.query.filter(w => w.finished === false && w.name === "RecycleTest").stream.toList)
      } yield {
        finished.status should be(WorkflowStatus.Success)
        finished.payloads(job.id) should be(str("em-elcycer"))
        // Fresh copy should exist, not finished, with same steps and name
        val fresh = all.filter(_._id != w._id)
        fresh.length should be(1)
        fresh.head.steps.length should be(3)
        fresh.head.finished should be(false)
        // The fresh copy has already started and reached the trigger (waiting state)
        fresh.head.waitingStepId should be(Some(trigger.id))
      }
    }
    "timeout a trigger with TimeoutAction.Skip" in {
      val trigger = TestTrigger(timeoutOverride = Some(500L), timeoutActionOverride = TimeoutAction.Skip)
      val afterJob = ReverseTextJob(Right("after-skip"))
      for {
        w <- WorkflowManager.schedule("TimeoutSkip", steps = List(trigger, afterJob), sourceId = testSourceId)
        _ <- WorkflowManager.waitForStatus(w._id, WorkflowStatus.Waiting)
        finished <- WorkflowManager.waitForFinished(w._id)
      } yield {
        finished.status should be(WorkflowStatus.Success)
        // After job should have run since Skip just skips the trigger step
        finished.payloads(afterJob.id) should be(str("piks-retfa"))
      }
    }
    "execute parallel branches" in {
      val job1 = ReverseTextJob(Right("branch-a"))
      val job2 = ReverseTextJob(Right("branch-b"))
      val parallel = Parallel(
        branches = List(List(job1.id), List(job2.id))
      )
      for {
        w <- WorkflowManager.schedule("ParallelTest", steps = List(job1, job2, parallel), sourceId = testSourceId)
        finished <- WorkflowManager.waitForFinished(w._id)
      } yield {
        finished.status should be(WorkflowStatus.Success)
        finished.payloads(job1.id) should be(str("a-hcnarb"))
        finished.payloads(job2.id) should be(str("b-hcnarb"))
      }
    }
    "execute loop over variable list" in {
      val bodyJob = ReverseTextJob(Right("placeholder"))
      val loop = Loop(
        itemsVariable = "items",
        bodySteps = List(bodyJob.id),
        itemVariableName = "item",
        outputVariable = "results"
      )
      for {
        w <- WorkflowManager.schedule("LoopTest",
          steps = List(bodyJob, loop),
          sourceId = testSourceId,
          variables = Map("items" -> fabric.arr(str("hello"), str("world")))
        )
        finished <- WorkflowManager.waitForFinished(w._id)
      } yield {
        finished.status should be(WorkflowStatus.Success)
        finished.variables.contains("results") should be(true)
      }
    }
    "coerce a non-array loop source (text blob / {text} / {results}) into items" in {
      // A discovery step's result reaches the loop as its `output` variable,
      // but it isn't always a JSON array: grep/glob yield a text blob, and
      // coalesced messages yield `{"results":[...]}`. The loop must iterate
      // those shapes so `find -> Loop -> act` composes without a hand-built
      // array. The loop's output collects one entry per iteration, so its
      // size is the item count.
      def loopCount(source: Json): rapid.Task[Int] = {
        val bodyJob = ReverseTextJob(Right("x"))
        val loop = Loop(
          itemsVariable = "src",
          bodySteps = List(bodyJob.id),
          itemVariableName = "item",
          outputVariable = "out"
        )
        for {
          w <- WorkflowManager.schedule("CoerceLoopTest",
            steps = List(bodyJob, loop),
            sourceId = testSourceId,
            variables = Map("src" -> source)
          )
          finished <- WorkflowManager.waitForFinished(w._id)
        } yield finished.variables.get("out").map(_.asVector.size).getOrElse(-1)
      }
      for {
        fromString  <- loopCount(str("a\nb\nc"))
        fromText    <- loopCount(obj("text" -> str("x\ny")))
        fromResults <- loopCount(obj("results" -> fabric.arr(str("p"), str("q"), str("r"), str("s"))))
        fromArray   <- loopCount(fabric.arr(str("only")))
      } yield {
        fromString should be(3)
        fromText should be(2)
        fromResults should be(4)
        fromArray should be(1)
      }
    }
    "emit step-completed events for loop body steps and the loop container" in {
      val bodyJob = ReverseTextJob(Right("loop-body"))
      val loop = Loop(
        itemsVariable = "items",
        bodySteps = List(bodyJob.id),
        itemVariableName = "item",
        outputVariable = "results"
      )
      hookEvents = Nil
      for {
        w <- WorkflowManager.schedule("LoopStepEvents353",
          steps = List(bodyJob, loop),
          sourceId = testSourceId,
          variables = Map("items" -> fabric.arr(str("a"), str("b"), str("c")))
        )
        finished <- WorkflowManager.waitForFinished(w._id)
      } yield {
        finished.status should be(WorkflowStatus.Success)
        // Before this fix the loop body emitted ZERO step-completed events; the run showed
        // started -> (nothing) -> completed. Now each iteration's body step fires (3 items, plus
        // the one top-level run of the same job = 4) and the Loop container fires exactly once.
        hookEvents.count(_.startsWith(s"step:${bodyJob.id.value}:")) should be >= 4
        hookEvents.count(_ == s"step:${loop.id.value}:true") should be(1)
      }
    }
    "write a top-level job's payload into its outputVariable" in {
      val job = ReverseTextJob(Right("threadme"), outVar = Some("r"))
      for {
        w <- WorkflowManager.schedule("JobOutputVar", steps = List(job), sourceId = testSourceId)
        finished <- WorkflowManager.waitForFinished(w._id)
      } yield {
        finished.status should be(WorkflowStatus.Success)
        // The payload is written to both payloads(stepId) and the named variable, so a later step
        // can reference it via {{r}}.
        finished.variables.get("r") should be(Some(str("emdaerht")))
        finished.payloads.get(job.id) should be(Some(str("emdaerht")))
      }
    }
    "reject resume on wrong stepId" in {
      val trigger = TestTrigger()
      for {
        w <- WorkflowManager.schedule("WrongStep", steps = List(trigger), sourceId = testSourceId)
        _ <- WorkflowManager.waitForStatus(w._id, WorkflowStatus.Waiting)
        result <- WorkflowManager.resume(w._id, Step.id(), str("wrong")).map(_ => "ok").handleError(_ => Task.pure("error"))
      } yield {
        result should be("error")
      }
    }
    "reject resume on non-waiting workflow" in {
      for {
        w <- WorkflowManager.schedule("NotWaiting", steps = List(ReverseTextJob(Right("x"))), sourceId = testSourceId)
        finished <- WorkflowManager.waitForFinished(w._id)
        result <- WorkflowManager.resume(w._id, Step.id(), str("nope")).map(_ => "ok").handleError(_ => Task.pure("error"))
      } yield {
        result should be("error")
      }
    }
    "handle branch trigger as first step with no prior state" in {
      val trigger = BranchTrigger()
      val job = ReverseTextJob(Right("first-step-branch"))
      for {
        w <- WorkflowManager.schedule("FirstStepBranch", steps = List(trigger, job), sourceId = testSourceId)
        _ <- WorkflowManager.waitForStatus(w._id, WorkflowStatus.Waiting)
        _ <- WorkflowManager.resume(w._id, trigger.id, str("payload"))
        _ <- Task.sleep(2.seconds)
        clones <- db.workflows.transaction(_.query.filter(_.parentRunId === Some(w._id)).stream.toList)
        original <- WorkflowManager.byId(w._id).map(_.get)
      } yield {
        // Original stays parked
        original.waitingStepId should be(Some(trigger.id))
        original.finished should be(false)
        // Clone completed
        clones.length should be(1)
        clones.head.status should be(WorkflowStatus.Success)
        clones.head.payloads(job.id) should be(str("hcnarb-pets-tsrif"))
      }
    }
    "handle workflow variables" in {
      val job = ReverseTextJob(Right("vars"))
      for {
        w <- WorkflowManager.schedule("VarsTest", steps = List(job), sourceId = testSourceId, variables = Map("key" -> str("value")))
        finished <- WorkflowManager.waitForFinished(w._id)
      } yield {
        finished.variables("key") should be(str("value"))
        finished.status should be(WorkflowStatus.Success)
      }
    }
    "execute a sub-workflow" in {
      implicit val sRW: RW[Step] = WorkflowModel.stepRW
      val innerJob = ReverseTextJob(Right("inner"))
      val sub = SubWorkflow(
        childName = "ChildWF",
        childSteps = List(innerJob),
        inputVariables = Map.empty
      )
      val outerJob = ReverseTextJob(Right("outer"))
      for {
        w <- WorkflowManager.schedule("SubWorkflowTest", steps = List(outerJob, sub), sourceId = testSourceId)
        finished <- WorkflowManager.waitForFinished(w._id)
      } yield {
        finished.status should be(WorkflowStatus.Success)
        finished.payloads(outerJob.id) should be(str("retuo"))
        // Child workflow output should be available as the sub-workflow step's payload
        finished.payloads(sub.id) should be(str("renni"))
        // Should have child workflow activity in history
        val childStarted = finished.history.exists(_.activity.isInstanceOf[WorkflowActivity.ChildWorkflowStarted])
        childStarted should be(true)
      }
    }

    "reject schedule with missing required variables" in {
      val result = WorkflowManager.schedule(
        "MissingVars",
        steps = List(ReverseTextJob(Right("x"))),
        sourceId = testSourceId,
        variableDefs = List(
          WorkflowVariable("required_var", required = true),
          WorkflowVariable("optional_var")
        )
      ).map(_ => "ok").handleError(_ => Task.pure("error"))
      result.map(_ should be("error"))
    }
    "apply default variable values on schedule" in {
      for {
        w <- WorkflowManager.schedule(
          "DefaultVars",
          steps = List(ReverseTextJob(Right("x"))),
          sourceId = testSourceId,
          variableDefs = List(
            WorkflowVariable("with_default", defaultValue = Some(str("default-value"))),
            WorkflowVariable("provided")
          ),
          variables = Map("provided" -> str("explicit"))
        )
        finished <- WorkflowManager.waitForFinished(w._id)
      } yield {
        finished.variables("with_default") should be(str("default-value"))
        finished.variables("provided") should be(str("explicit"))
      }
    }
    "not override provided variables with defaults" in {
      for {
        w <- WorkflowManager.schedule(
          "NoOverride",
          steps = List(ReverseTextJob(Right("x"))),
          sourceId = testSourceId,
          variableDefs = List(
            WorkflowVariable("key", defaultValue = Some(str("default")))
          ),
          variables = Map("key" -> str("override"))
        )
        finished <- WorkflowManager.waitForFinished(w._id)
      } yield {
        finished.variables("key") should be(str("override"))
      }
    }
    "execute parallel with JoinMode.Any returning first completed" in {
      val fastJob = ReverseTextJob(Right("fast"))
      val slowJob = TimedJob(2.seconds)
      val parallel = Parallel(
        branches = List(List(fastJob.id), List(slowJob.id)),
        joinMode = JoinMode.Any
      )
      for {
        w <- WorkflowManager.schedule("ParallelAny", steps = List(fastJob, slowJob, parallel), sourceId = testSourceId)
        finished <- WorkflowManager.waitForFinished(w._id)
      } yield {
        finished.status should be(WorkflowStatus.Success)
        // Fast job should have completed, its payload should be present
        finished.payloads(fastJob.id) should be(str("tsaf"))
      }
    }
    "verify StepResult is populated on successful job execution" in {
      for {
        w <- WorkflowManager.schedule("StepResultTest", steps = List(ReverseTextJob(Right("result"))), sourceId = testSourceId)
        finished <- WorkflowManager.waitForFinished(w._id)
      } yield {
        finished.stepResults.length should be(1)
        val result = finished.stepResults.head
        result.stepName should be("ReverseTextJob")
        result.status should be(StepResultStatus.Completed)
        result.output should be(Some(str("tluser")))
        result.durationMs should be >= 0L
        result.error should be(None)
      }
    }
    "verify StepResult records failure with error message" in {
      for {
        w <- WorkflowManager.schedule("StepResultFail", steps = List(FailingJob()), sourceId = testSourceId)
        finished <- WorkflowManager.waitForFinished(w._id)
      } yield {
        finished.stepResults.length should be(1)
        val result = finished.stepResults.head
        result.stepName should be("FailingJob")
        result.status should be(StepResultStatus.Failed)
        result.error.isDefined should be(true)
        result.output should be(None)
      }
    }
    "verify StepResult on continueOnError records failure then next step succeeds" in {
      val failJob = ContinueOnErrorJob()
      val nextJob = ReverseTextJob(Right("after"))
      for {
        w <- WorkflowManager.schedule("StepResultContinue", steps = List(failJob, nextJob), sourceId = testSourceId)
        finished <- WorkflowManager.waitForFinished(w._id)
      } yield {
        finished.stepResults.length should be(2)
        // Results are prepended, so most recent is first
        val failResult = finished.stepResults.find(_.stepId == failJob.id).get
        failResult.status should be(StepResultStatus.Failed)
        failResult.error.isDefined should be(true)
        val successResult = finished.stepResults.find(_.stepId == nextJob.id).get
        successResult.status should be(StepResultStatus.Completed)
      }
    }
    "generate WorkflowPreview with correct fields" in {
      for {
        w <- WorkflowManager.schedule("PreviewTest", steps = List(TimedJob(500.millis)), sourceId = testSourceId)
        _ <- WorkflowManager.waitForStatus(w._id, WorkflowStatus.Running)
        running <- WorkflowManager.byId(w._id).map(_.get)
        preview = running.preview
        finished <- WorkflowManager.waitForFinished(w._id)
        finishedPreview = finished.preview
      } yield {
        preview.name should be("PreviewTest")
        preview.steps.length should be(1)
        preview.workflowId should be(w._id)
        preview.status should be(WorkflowStatus.Running)
        finishedPreview.status should be(WorkflowStatus.Success)
        finishedPreview.finished should be(true)
      }
    }
    "restart manager for isolated tests" in {
      for {
        _ <- WorkflowManager.dispose()
        _ <- db.workflows.transaction(_.truncate)
        _ <- WorkflowManager.init()
      } yield succeed
    }
    "recover crashed running workflow on manager restart" in {
      // Schedule a long-running job, kill the manager, restart, verify recovery
      for {
        w <- WorkflowManager.schedule("CrashRunning", steps = List(TimedJob(10.seconds)), sourceId = testSourceId)
        _ <- WorkflowManager.waitForStatus(w._id, WorkflowStatus.Running)
        // Kill the manager without letting the job finish
        _ <- WorkflowManager.dispose()
        _ <- Task.sleep(500.millis) // Allow fiber cancellation to settle
        // Reinitialize — should detect the crashed workflow and mark it failed
        _ <- WorkflowManager.init()
        recovered <- WorkflowManager.byId(w._id).map(_.get)
      } yield {
        recovered.finished should be(true)
        recovered.status should be(WorkflowStatus.Failure)
        val failHistory = recovered.history.collect { case h if h.activity.isInstanceOf[WorkflowActivity.StepFailure] => h.activity.asInstanceOf[WorkflowActivity.StepFailure] }
        failHistory.length should be(1)
        failHistory.head.errorMessage should include("Server stopped")
      }
    }
    "recover crashed waiting workflow on manager restart" in {
      val trigger = TestTrigger()
      for {
        w <- WorkflowManager.schedule("CrashWaiting", steps = List(trigger, ReverseTextJob(Right("never"))), sourceId = testSourceId)
        _ <- WorkflowManager.waitForStatus(w._id, WorkflowStatus.Waiting)
        // Kill the manager
        _ <- WorkflowManager.dispose()
        _ <- Task.sleep(500.millis)
        // Reinitialize — should detect waiting workflow and fail it
        _ <- WorkflowManager.init()
        recovered <- WorkflowManager.byId(w._id).map(_.get)
      } yield {
        recovered.finished should be(true)
        recovered.status should be(WorkflowStatus.Failure)
        recovered.waitingStepId should be(None)
        val failHistory = recovered.history.collect { case h if h.activity.isInstanceOf[WorkflowActivity.StepFailure] => h.activity.asInstanceOf[WorkflowActivity.StepFailure] }
        failHistory.length should be(1)
        failHistory.head.errorMessage should include("Server stopped")
      }
    }

    "use exponential backoff on retries" in {
      val start = System.currentTimeMillis()
      for {
        w <- WorkflowManager.schedule("ExponentialBackoff", steps = List(
          ExponentialBackoffJob(failTimes = 2)
        ), sourceId = testSourceId)
        finished <- WorkflowManager.waitForFinished(w._id)
      } yield {
        finished.status should be(WorkflowStatus.Success)
        val elapsed = System.currentTimeMillis() - start
        // With base delay 100ms and exponential: attempt 0 = 100ms, attempt 1 = 200ms = 300ms total minimum
        elapsed should be >= 300L
      }
    }
    "timeout a long-running job with step-level executionTimeoutMs" in {
      for {
        w <- WorkflowManager.schedule("StepTimeout", steps = List(TimeoutJob()), sourceId = testSourceId)
        finished <- WorkflowManager.waitForFinished(w._id)
      } yield {
        finished.status should be(WorkflowStatus.Failure)
        val failure = finished.history.collect { case h if h.activity.isInstanceOf[WorkflowActivity.StepFailure] => h.activity.asInstanceOf[WorkflowActivity.StepFailure] }
        failure.head.errorMessage should include("timed out")
      }
    }
    "enforce workflow-level timeout" in {
      // Three jobs that each take 300ms = 900ms total. Workflow timeout is 500ms.
      // The timeout is checked between steps.
      for {
        w <- WorkflowManager.schedule("WorkflowTimeout",
          steps = List(TimedJob(300.millis), TimedJob(300.millis), TimedJob(300.millis)),
          sourceId = testSourceId,
          workflowTimeoutMs = Some(500L)
        )
        finished <- WorkflowManager.waitForFinished(w._id)
      } yield {
        finished.status should be(WorkflowStatus.Failure)
        val timedOut = finished.history.exists(_.activity.isInstanceOf[WorkflowActivity.TimedOut])
        timedOut should be(true)
      }
    }
    "schedule with tags and list by tag" in {
      for {
        _ <- WorkflowManager.schedule("Tagged1", steps = List(ReverseTextJob(Right("a"))), sourceId = testSourceId, tags = Set("nightly", "import"))
        _ <- WorkflowManager.schedule("Tagged2", steps = List(ReverseTextJob(Right("b"))), sourceId = testSourceId, tags = Set("nightly"))
        _ <- WorkflowManager.schedule("Untagged", steps = List(ReverseTextJob(Right("c"))), sourceId = testSourceId)
        _ <- Task.sleep(2.seconds) // Let them finish
        nightlyList <- WorkflowManager.list(tag = Some("nightly"))
        importList <- WorkflowManager.list(tag = Some("import"))
        allList <- WorkflowManager.list(name = Some("Tagged"))
      } yield {
        nightlyList.length should be(2)
        importList.length should be(1)
        allList.length should be(2)
      }
    }
    "deduplicate with scheduleIfNotActive" in {
      val trigger = TestTrigger()
      for {
        first <- WorkflowManager.scheduleIfNotActive("DedupeTest", steps = List(trigger), sourceId = testSourceId, tags = Set("dedup"))
        second <- WorkflowManager.scheduleIfNotActive("DedupeTest", steps = List(trigger), sourceId = testSourceId, tags = Set("dedup"))
      } yield {
        first.isDefined should be(true)
        second should be(None)
      }
    }
    "cancelAll by name" in {
      val trigger1 = TestTrigger()
      val trigger2 = TestTrigger()
      for {
        w1 <- WorkflowManager.schedule("CancelAllTest", steps = List(trigger1), sourceId = testSourceId)
        _ <- WorkflowManager.waitForStatus(w1._id, WorkflowStatus.Waiting)
        w2 <- WorkflowManager.schedule("CancelAllTest", steps = List(trigger2), sourceId = testSourceId)
        _ <- WorkflowManager.waitForStatus(w2._id, WorkflowStatus.Waiting)
        cancelled <- WorkflowManager.cancelAll(name = Some("CancelAllTest"))
      } yield {
        cancelled.length should be(2)
        cancelled.head.finished should be(true)
        cancelled.last.finished should be(true)
      }
    }
    "call lifecycle hooks on completion" in {
      hookEvents = Nil
      for {
        w <- WorkflowManager.schedule("HooksTest", steps = List(ReverseTextJob(Right("hooks"))), sourceId = testSourceId)
        finished <- WorkflowManager.waitForFinished(w._id)
        _ <- Task.sleep(200.millis) // Allow async hooks to complete
      } yield {
        finished.status should be(WorkflowStatus.Success)
        hookEvents should contain(s"completed:${w._id.value}")
        hookEvents should contain(s"step:${finished.steps.head.id.value}:true")
      }
    }
    "call lifecycle hooks on failure" in {
      hookEvents = Nil
      for {
        w <- WorkflowManager.schedule("HooksFailTest", steps = List(FailingJob()), sourceId = testSourceId)
        finished <- WorkflowManager.waitForFinished(w._id)
        _ <- Task.sleep(200.millis)
      } yield {
        finished.status should be(WorkflowStatus.Failure)
        hookEvents should contain(s"failed:${w._id.value}")
        hookEvents should contain(s"step:${finished.steps.head.id.value}:false")
      }
    }

    "schedule a workflow where the job throws inside handleError" in {
      val job = ThrowInHandleErrorJob()
      for {
        w <- WorkflowManager.schedule("ThrowInHandleError", steps = List(job), sourceId = testSourceId)
        finished <- WorkflowManager.waitForFinished(w._id, delay = 500.millis)
      } yield {
        finished.status should be(WorkflowStatus.Failure)
        finished.finished should be(true)
        finished.runningId should be(None)
      }
    }
    "schedule a workflow where the job succeeds after catching an internal stream error" in {
      val job = StreamErrorButSucceedsJob()
      for {
        w <- WorkflowManager.schedule("StreamErrorButSucceeds", steps = List(job), sourceId = testSourceId)
        finished <- WorkflowManager.waitForFinished(w._id, delay = 500.millis)
      } yield {
        finished.status should be(WorkflowStatus.Success)
        finished.finished should be(true)
        finished.runningId should be(None)
      }
    }
    "schedule a workflow where the job errors from a nested stream" in {
      val job = NestedStreamErrorJob()
      for {
        w <- WorkflowManager.schedule("NestedStreamError", steps = List(job), sourceId = testSourceId)
        finished <- WorkflowManager.waitForFinished(w._id, delay = 500.millis)
      } yield {
        finished.status should be(WorkflowStatus.Failure)
        finished.finished should be(true)
        finished.runningId should be(None)
      }
    }
    "dispose manager" in {
      WorkflowManager.dispose().map(_ => succeed)
    }
    "dispose database" in {
      db.dispose.succeed
    }
  }
}

object db extends LightDB {
  override type SM = SplitStoreManager[RocksDBStore.type, LuceneStore.type]
  override val storeManager: SM = SplitStoreManager(RocksDBStore, LuceneStore)

  val workflows: Collection[Workflow, WorkflowModel.type] = store(WorkflowModel)()

  override def directory: Option[Path] = Some(Path.of("db", "workflows"))
  override def upgrades: List[DatabaseUpgrade] = Nil
}

object WorkflowModel extends AbstractWorkflowModel {
  // SubWorkflow contains List[Step] which needs stepRW, creating a circular init.
  // We use RW.poly's lazy resolution — SubWorkflow's RW is defined after stepRW but
  // registered as part of the poly which resolves lazily at first use (after init).
  override implicit lazy val stepRW: RW[Step] = RW.poly()(
    RW.gen[ReverseTextJob], RW.gen[FailingJob], RW.gen[TimedJob],
    RW.gen[RetryableJob], RW.gen[ContinueOnErrorJob], RW.gen[ExponentialBackoffJob], RW.gen[TimeoutJob],
    RW.gen[ThrowInHandleErrorJob], RW.gen[StreamErrorButSucceedsJob], RW.gen[NestedStreamErrorJob],
    RW.gen[TestTrigger], RW.gen[TestCondition], RW.gen[TestApproval], RW.gen[BranchTrigger],
    summon[RW[Parallel]], summon[RW[Loop]], summon[RW[Recycle]], SubWorkflow.rw
  )
}

val testSourceId: Id[WorkflowParent] = Id()
var hookEvents = List.empty[String]

object WorkflowManager extends AbstractWorkflowManager[WorkflowParent, WorkflowModel.type](db.workflows) {
  override protected def resolveParent(sourceId: Id[WorkflowParent]): Task[Option[WorkflowParent]] = Task.pure(None)
  override protected def onWorkflowCompleted(workflow: Workflow): Task[Unit] = Task {
    hookEvents = s"completed:${workflow._id.value}" :: hookEvents
  }
  override protected def onWorkflowFailed(workflow: Workflow): Task[Unit] = Task {
    hookEvents = s"failed:${workflow._id.value}" :: hookEvents
  }
  override protected def onStepCompleted(workflow: Workflow, stepId: Id[Step], success: Boolean): Task[Unit] = Task {
    hookEvents = s"step:${stepId.value}:$success" :: hookEvents
  }
}

case class ReverseTextJob(text: Either[Id[Step], String],
                          id: Id[Step] = Step.id(),
                          outVar: Option[String] = None) extends Job[String] {
  override def outputVariable: Option[String] = outVar
  override def execute(workflow: Workflow, pm: ProgressManager): Task[String] = Task {
    val string = text match {
      case Left(jobId) => workflow.payloads(jobId).asString
      case Right(s) => s
    }
    string.reverse
  }
}

case class FailingJob(id: Id[Step] = Step.id()) extends Job[Json] {
  override def execute(workflow: Workflow, pm: ProgressManager): Task[Json] =
    Task.error(new RuntimeException("Expected failure!"))
}

case class TimedJob(time: FiniteDuration, override val weight: Double = 1.0, id: Id[Step] = Step.id()) extends Job[Json] {
  override def execute(workflow: Workflow, pm: ProgressManager): Task[Json] = {
    val millis = time.toMillis / 10
    (0 until 10).toList.map { index =>
      pm.percentage(index, 10)
      Task.sleep(millis.millis)
    }.tasks.map(_ => Null)
  }
}

case class RetryableJob(failTimes: Int, id: Id[Step] = Step.id()) extends Job[String] {
  @volatile private var attempts = 0

  override def retryCount: Int = failTimes
  override def retryDelayMs: Long = 100L

  override def execute(workflow: Workflow, pm: ProgressManager): Task[String] = Task {
    attempts += 1
    if (attempts <= failTimes) {
      throw new RuntimeException(s"Failing attempt $attempts")
    }
    s"success after $attempts attempts"
  }
}

case class ContinueOnErrorJob(id: Id[Step] = Step.id()) extends Job[Json] {
  override def continueOnError: Boolean = true

  override def execute(workflow: Workflow, pm: ProgressManager): Task[Json] =
    Task.error(new RuntimeException("Expected failure, but continue"))
}

case class TestTrigger(timeoutOverride: Option[Long] = None,
                       timeoutActionOverride: TimeoutAction = TimeoutAction.Fail,
                       id: Id[Step] = Step.id()) extends Trigger {
  override def timeoutMs: Option[Long] = timeoutOverride
  override def timeoutAction: TimeoutAction = timeoutActionOverride

  override def register(workflow: Workflow): Task[Json] = Task.pure(str("registered"))
  override def check(workflow: Workflow): Task[Option[Json]] = Task.pure(None) // Never auto-resolves
}

case class TestCondition(result: Boolean,
                         onTrue: Id[Step],
                         onFalse: Id[Step],
                         id: Id[Step] = Step.id()) extends Condition {
  override def evaluate(workflow: Workflow): Task[Boolean] = Task.pure(result)
}

case class TestApproval(id: Id[Step] = Step.id()) extends Approval {
  override def prompt: String = "Please approve"
  override def options: List[String] = List("Approve", "Reject")
}

case class ExponentialBackoffJob(failTimes: Int, id: Id[Step] = Step.id()) extends Job[String] {
  @volatile private var attempts = 0
  override def retryCount: Int = failTimes
  override def retryDelayMs: Long = 100L
  override def retryBackoff: RetryBackoff = RetryBackoff.Exponential

  override def execute(workflow: Workflow, pm: ProgressManager): Task[String] = Task {
    attempts += 1
    if (attempts <= failTimes) throw new RuntimeException(s"Fail $attempts")
    s"ok after $attempts"
  }
}

case class TimeoutJob(id: Id[Step] = Step.id()) extends Job[Json] {
  override def executionTimeoutMs: Option[Long] = Some(200L)

  override def execute(workflow: Workflow, pm: ProgressManager): Task[Json] = {
    Task.sleep(10.seconds).map(_ => Null)
  }
}

case class BranchTrigger(id: Id[Step] = Step.id()) extends Trigger {
  override def mode: TriggerMode = TriggerMode.Branch
  override def register(workflow: Workflow): Task[Json] = Task.pure(str("branch-registered"))
  override def check(workflow: Workflow): Task[Option[Json]] = Task.pure(None) // Never auto-resolves
}

/** Mimics dialect pattern: handleError catches, logs, and re-throws */
case class ThrowInHandleErrorJob(id: Id[Step] = Step.id()) extends Job[Json] {
  override def execute(workflow: Workflow, pm: ProgressManager): Task[Json] = {
    Task.error[Json](new RuntimeException("inner error")).handleError { t =>
      scribe.error("Caught error, re-throwing", t)
      Task.error(t)
    }
  }
}

/** Job succeeds even though an internal stream element errored (error was caught internally) */
case class StreamErrorButSucceedsJob(id: Id[Step] = Step.id()) extends Job[Json] {
  override def execute(workflow: Workflow, pm: ProgressManager): Task[Json] = {
    var count = 0
    rapid.Stream(1, 2, 3).evalMap { i =>
      if (i == 2) Task.error(new RuntimeException("element 2 failed"))
      else Task { count += 1; i }
    }.drain.handleError { _ =>
      Task.pure(()) // Swallow stream error
    }.map(_ => fabric.num(count))
  }
}

/** Job that errors from a nested stream — the error should propagate and fail the workflow */
case class NestedStreamErrorJob(id: Id[Step] = Step.id()) extends Job[Json] {
  override def execute(workflow: Workflow, pm: ProgressManager): Task[Json] = {
    rapid.Stream(1, 2, 3).evalMap { i =>
      if (i == 2) Task.error(new RuntimeException("nested stream error on element 2"))
      else Task(i)
    }.drain.map(_ => fabric.Null).guarantee(Task {
      scribe.info("NestedStreamErrorJob guarantee block ran")
    }).handleError { t =>
      scribe.error("NestedStreamErrorJob caught error, re-throwing", t)
      Task.error(t)
    }
  }
}