package strider

import strider.step.*

import fabric.{Json, Null, num}
import fabric.rw._
import lightdb.store.Collection
import reactify.{Channel, Val, Var}
import lightdb.Sort
import lightdb.filter.FilterExtras
import lightdb.time.Timestamp
import lightdb.id.Id
import lightdb.progress.{Progress, ProgressManager}
import lightdb.transaction.Transaction
import lightdb.trigger.StoreTrigger
import spice.UserException
import rapid.{Fiber, FiberOps, Task, logger, RapidApp}

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.{DurationInt, DurationLong, FiniteDuration}
import scala.math.Ordered.orderingToOrdered

abstract class AbstractWorkflowManager[Parent <: WorkflowParent, WorkflowModel <: AbstractWorkflowModel](
    val collection: Collection[Workflow, WorkflowModel],
    maxConcurrentWorkflows: Int = 1
) {
  @volatile private var keepAlive = true
  @volatile private var monitorFiber: Fiber[Unit] = _

  val paused: Var[Boolean] = Var(false)

  private val changed = Var(true)
  private val activeCount = new AtomicInteger(0)

  /** Resolve the WorkflowParent (template) for a given sourceId. Override to provide storage lookup. */
  protected def resolveParent(sourceId: Id[WorkflowParent]): Task[Option[Parent]]

  /** Called when a workflow first transitions from scheduled to running.
    * Sigil/Strider downstream consumers fold this into their event log
    * so the run's start is visible alongside its later step / completion
    * events. Default no-op. */
  protected def onWorkflowStarted(workflow: Workflow): Task[Unit] = Task.unit

  /** Called when a workflow completes successfully. Override to add custom behavior. */
  protected def onWorkflowCompleted(workflow: Workflow): Task[Unit] = Task.unit

  /** Called when a workflow fails. Override to add custom behavior. */
  protected def onWorkflowFailed(workflow: Workflow): Task[Unit] = Task.unit

  /** Called when a step is about to execute. Override to add custom
    * behavior — Sigil uses this to surface a "step started" Event in
    * the originating conversation. Default no-op. */
  protected def onStepStarted(workflow: Workflow, stepId: Id[Step]): Task[Unit] = Task.unit

  /** Called when a step completes (success or failure). Override to add custom behavior. */
  protected def onStepCompleted(workflow: Workflow, stepId: Id[Step], success: Boolean): Task[Unit] = Task.unit

  def init(): Task[Unit] = for {
    _ <- logger.info("Initializing Workflow Manager...")
    _ = keepAlive = true
    _ = activeCount.set(0)
    _ = collection.trigger += new StoreTrigger[Workflow, WorkflowModel] {
      override def insert(doc: Workflow, transaction: Transaction[Workflow, WorkflowModel]): Task[Unit] = Task(changed @= true)
      override def upsert(doc: Workflow, transaction: Transaction[Workflow, WorkflowModel]): Task[Unit] = Task(changed @= true)
    }
    _ <- collection.transaction { txn =>
      txn.stream
        .filter(w => w.runningId.isDefined || w.waitingStepId.isDefined)
        .foreach { w =>
          scribe.info(s"Recovering workflow: ${w.name} (created: ${w.created}, runningId: ${w.runningId}, waitingStepId: ${w.waitingStepId})")
        }
        .evalMap { workflow =>
          if (workflow.runningId.isDefined) {
            scribe.info(s"Failing crashed workflow: ${workflow.name}")
            modify(workflow._id, txn) { workflow =>
              val stepFailure = workflow.runningId match {
                case Some(stepId) => List(WorkflowHistory(WorkflowActivity.StepFailure(stepId, "Server stopped while step running")))
                case None => Nil
              }
              val completed = workflow.runningId match {
                case Some(stepId) => stepId :: workflow.completed
                case None => workflow.completed
              }
              val workflowCompleted = WorkflowHistory(WorkflowActivity.Completed(false))
              Task.pure(workflow.copy(
                runningId = None,
                completed = completed,
                history = workflowCompleted :: stepFailure ::: workflow.history
              ))
            }
          } else {
            // Workflow was waiting on a trigger/approval — clean up and fail
            scribe.info(s"Failing waiting workflow after restart: ${workflow.name}")
            unregisterIfTrigger(workflow, workflow.waitingStepId.get).flatMap { _ =>
              modify(workflow._id, txn) { workflow =>
                Task.pure(workflow.copy(
                  waitingStepId = None,
                  waitingSince = None,
                  history =
                    WorkflowHistory(WorkflowActivity.Completed(false)) ::
                      WorkflowHistory(WorkflowActivity.StepFailure(
                        workflow.waitingStepId.get,
                        "Server stopped while waiting"
                      )) :: workflow.history
                ))
              }
            }
          }
        }
        .drain
    }
    _ <- collection.reIndex()
    fiber = monitor(0L).start()
  } yield {
    monitorFiber = fiber
  }

  private def monitor(nextCheck: Long): Task[Unit] = if (!keepAlive) {
    Task.unit
  } else if ((changed() || System.currentTimeMillis() >= nextCheck) && !paused()) {
    for {
      _ <- checkWaitingWorkflows()
      next <- runNextScheduled()
      _ <- monitor(next)
    } yield ()
  } else {
    Task.sleep(500.millis).flatMap(_ => monitor(nextCheck))
  }

  private def checkWaitingWorkflows(): Task[Unit] = collection.transaction { txn =>
    txn.query
      .filter(w => w.finished === false)
      .stream
      .filter(_.waitingStepId.isDefined)
      .evalMap { workflow =>
        val stepId = workflow.waitingStepId.get
        workflow.byStepId(stepId) match {
          case Some(trigger: Trigger) =>
            trigger.check(workflow).flatMap {
              case Some(payload) =>
                resumeFromWait(workflow, stepId, payload, txn)
              case None =>
                checkStepTimeout(workflow, stepId, trigger.timeoutMs, trigger.timeoutAction, txn)
            }
          case Some(approval: Approval) =>
            checkStepTimeout(workflow, stepId, approval.timeoutMs, approval.timeoutAction, txn)
          case _ => Task.unit
        }
      }
      .drain
  }

  private def checkStepTimeout(workflow: Workflow,
                                stepId: Id[Step],
                                timeoutMs: Option[Long],
                                timeoutAction: TimeoutAction,
                                txn: Transaction[Workflow, WorkflowModel]): Task[Unit] = {
    (timeoutMs, workflow.waitingSince) match {
      case (Some(timeout), Some(since)) if System.currentTimeMillis() - since >= timeout =>
        handleTimeout(workflow, stepId, timeoutAction, txn).map(_ => ())
      case _ => Task.unit
    }
  }

  private def handleTimeout(workflow: Workflow,
                             stepId: Id[Step],
                             action: TimeoutAction,
                             txn: Transaction[Workflow, WorkflowModel]): Task[Workflow] = {
    // Unregister trigger if applicable
    unregisterIfTrigger(workflow, stepId).flatMap { _ =>
      action match {
        case TimeoutAction.Proceed =>
          addHistory(workflow._id, WorkflowActivity.TimedOut(Some(stepId)), txn).flatMap { w =>
            modify(w._id, txn) { wf =>
              Task.pure(wf.copy(
                waitingStepId = None,
                waitingSince = None,
                completed = stepId :: wf.completed,
                payloads = wf.payloads + (stepId -> Null)
              ))
            }
          }.flatMap { wf =>
            changed @= true
            Task.pure(wf)
          }
        case TimeoutAction.Skip =>
          addHistory(workflow._id, WorkflowActivity.TimedOut(Some(stepId)), txn).flatMap { w =>
            modify(w._id, txn) { wf =>
              Task.pure(wf.copy(
                waitingStepId = None,
                waitingSince = None
              ))
            }
          }.flatMap { wf =>
            changed @= true
            Task.pure(wf)
          }
        case TimeoutAction.Fail =>
          modify(workflow._id, txn) { wf =>
            Task.pure(wf.copy(
              waitingStepId = None,
              waitingSince = None,
              history =
                WorkflowHistory(WorkflowActivity.Completed(false)) ::
                  WorkflowHistory(WorkflowActivity.TimedOut(Some(stepId))) :: wf.history
            ))
          }
      }
    }
  }

  private def resumeFromWait(workflow: Workflow,
                              stepId: Id[Step],
                              payload: Json,
                              txn: Transaction[Workflow, WorkflowModel]): Task[Unit] = {
    workflow.byStepId(stepId) match {
      case Some(trigger: Trigger) if trigger.mode == TriggerMode.Branch =>
        branchFromTrigger(workflow, trigger, payload, txn)
      case _ =>
        continueFromWait(workflow, stepId, payload, txn)
    }
  }

  private def continueFromWait(workflow: Workflow,
                                stepId: Id[Step],
                                payload: Json,
                                txn: Transaction[Workflow, WorkflowModel]): Task[Unit] = {
    unregisterIfTrigger(workflow, stepId).flatMap { _ =>
      for {
        _ <- addHistory(workflow._id, WorkflowActivity.Resumed(stepId, payload), txn)
        wf <- modify(workflow._id, txn) { wf =>
          Task.pure(wf.copy(
            waitingStepId = None,
            waitingSince = None,
            completed = stepId :: wf.completed,
            payloads = wf.payloads + (stepId -> payload)
          ))
        }
        _ = changed @= true
        _ <- executeWorkflow(wf, txn)
      } yield ()
    }
  }

  private def branchFromTrigger(workflow: Workflow,
                                 trigger: Trigger,
                                 payload: Json,
                                 txn: Transaction[Workflow, WorkflowModel]): Task[Unit] = {
    // Clone the workflow at its current state — the clone continues, the original stays parked
    val clone = workflow.copy(
      waitingStepId = None,
      waitingSince = None,
      completed = trigger.id :: workflow.completed,
      payloads = workflow.payloads + (trigger.id -> payload),
      parentRunId = Some(workflow._id),
      history = WorkflowHistory(WorkflowActivity.Resumed(trigger.id, payload)) :: workflow.history,
      _id = Id()
    )
    for {
      inserted <- collection.transaction(_.insert(clone))
      _ = changed @= true
    } yield ()
  }

  /**
   * Resume a workflow that is waiting on a trigger or approval.
   */
  def resume(workflowId: Id[Workflow], stepId: Id[Step], payload: Json): Task[Workflow] = {
    collection.transaction { txn =>
      txn.get(workflowId).flatMap {
        case Some(workflow) if workflow.waitingStepId.contains(stepId) =>
          resumeFromWait(workflow, stepId, payload, txn).flatMap(_ => txn.get(workflowId).map(_.get))
        case Some(_) =>
          Task.error(UserException(s"Workflow $workflowId is not waiting on step $stepId"))
        case None =>
          Task.error(UserException(s"Workflow $workflowId not found"))
      }
    }
  }

  /**
   * Cancel a workflow. Unregisters any active triggers and marks the workflow as cancelled.
   */
  def cancel(workflowId: Id[Workflow]): Task[Workflow] = {
    collection.transaction { txn =>
      txn.get(workflowId).flatMap {
        case Some(workflow) if !workflow.finished =>
          val unregTask = workflow.waitingStepId.map(sid => unregisterIfTrigger(workflow, sid)).getOrElse(Task.unit)
          unregTask.flatMap { _ =>
            modify(workflow._id, txn) { wf =>
              Task.pure(wf.copy(
                runningId = None,
                waitingStepId = None,
                waitingSince = None,
                history =
                  WorkflowHistory(WorkflowActivity.Completed(false)) ::
                    WorkflowHistory(WorkflowActivity.Cancelled) :: wf.history
              ))
            }
          }
        case Some(workflow) =>
          Task.pure(workflow) // Already finished
        case None =>
          Task.error(UserException(s"Workflow $workflowId not found"))
      }
    }
  }

  private def runNextScheduled(): Task[Long] = collection.transaction { txn =>
    if (activeCount.get() >= maxConcurrentWorkflows) {
      Task.pure(System.currentTimeMillis() + 1_000L)
    } else {
      changed @= false
      txn.query
        .filter { w =>
          w.finished === false && w.runningId === None && w.waitingStepId === None
        }
        .sort(
          Sort.ByField(collection.model.priority).desc,
          Sort.ByField(collection.model.scheduled).asc
        )
        .limit(1)
        .firstOption
        .flatMap {
          case Some(workflow) if workflow.scheduled > System.currentTimeMillis() =>
            Task.pure(workflow.scheduled)
          case Some(workflow) =>
            scribe.info(s"Executing ${workflow.name}")
            activeCount.incrementAndGet()
            changed @= true
            executeWorkflow(workflow, txn).map { _ =>
              activeCount.decrementAndGet()
              scribe.info(s"Finished execute for ${workflow.name}")
              0L
            }.handleError { throwable =>
              activeCount.decrementAndGet()
              scribe.error(s"Workflow ${workflow.name} failed unexpectedly", throwable)
              Task.pure(0L)
            }
          case None =>
            Task.pure(System.currentTimeMillis() + 5_000L)
        }
    }
  }

  def schedule(name: String,
               steps: List[Step],
               sourceId: Id[WorkflowParent],
               timeStamp: Long = System.currentTimeMillis(),
               priority: Double = 1.0,
               variables: Map[String, Json] = Map.empty,
               variableDefs: List[WorkflowVariable] = Nil,
               tags: Set[String] = Set.empty,
               workflowTimeoutMs: Option[Long] = None): Task[Workflow] = {
    val workflow = Workflow(
      name = name,
      steps = steps,
      scheduled = timeStamp,
      queue = steps.map(_.id),
      sourceId = sourceId,
      variableDefs = variableDefs,
      priority = priority,
      variables = variables,
      tags = tags,
      workflowTimeoutMs = workflowTimeoutMs,
      history = List(
        WorkflowHistory(WorkflowActivity.Scheduled(timeStamp)),
        WorkflowHistory(WorkflowActivity.Created)
      )
    ).withVariableDefaults
    val missing = workflow.validateVariables
    if (missing.nonEmpty) {
      Task.error(UserException(s"Missing required variables: ${missing.mkString(", ")}"))
    } else {
      collection.transaction(_.insert(workflow).flatTap(_ => Task(changed @= true)))
    }
  }

  def scheduleIn(name: String,
                 steps: List[Step],
                 sourceId: Id[WorkflowParent],
                 duration: FiniteDuration,
                 priority: Double = 1.0,
                 variables: Map[String, Json] = Map.empty,
                 variableDefs: List[WorkflowVariable] = Nil,
                 tags: Set[String] = Set.empty,
                 workflowTimeoutMs: Option[Long] = None): Task[Workflow] = schedule(
    name = name,
    steps = steps,
    sourceId = sourceId,
    timeStamp = System.currentTimeMillis() + duration.toMillis,
    priority = priority,
    variables = variables,
    variableDefs = variableDefs,
    tags = tags,
    workflowTimeoutMs = workflowTimeoutMs
  )

  def byId(workflowId: Id[Workflow]): Task[Option[Workflow]] = collection.transaction(_.get(workflowId))

  /**
   * List workflows matching optional filters.
   */
  def list(status: Option[WorkflowStatus] = None,
           tag: Option[String] = None,
           name: Option[String] = None,
           limit: Int = 50): Task[List[Workflow]] = collection.transaction { txn =>
    txn.query
      .sort(Sort.ByField(collection.model.scheduled).desc)
      .stream
      .filter { wf =>
        status.forall(_ == wf.status) &&
        tag.forall(wf.tags.contains) &&
        name.forall(wf.name.contains)
      }
      .take(limit)
      .toList
  }

  /**
   * Check if a workflow with the given name and tags is already active (not finished).
   * Useful for deduplication — call before schedule() to prevent duplicates.
   */
  def existsActive(name: String, tags: Set[String] = Set.empty): Task[Boolean] = collection.transaction { txn =>
    txn.query
      .filter(w => w.finished === false && w.name === name)
      .stream
      .filter(wf => tags.isEmpty || tags.subsetOf(wf.tags))
      .firstOption
      .map(_.isDefined)
  }

  /**
   * Schedule only if no active workflow with the same name exists.
   * Returns None if a duplicate was detected.
   */
  def scheduleIfNotActive(name: String,
                          steps: List[Step],
                          sourceId: Id[WorkflowParent],
                          timeStamp: Long = System.currentTimeMillis(),
                          priority: Double = 1.0,
                          variables: Map[String, Json] = Map.empty,
                          variableDefs: List[WorkflowVariable] = Nil,
                          tags: Set[String] = Set.empty): Task[Option[Workflow]] = {
    existsActive(name, tags).flatMap {
      case true => Task.pure(None)
      case false => schedule(name, steps, sourceId, timeStamp, priority, variables, variableDefs, tags).map(Some(_))
    }
  }

  /**
   * Cancel all active workflows matching the given filters.
   */
  def cancelAll(name: Option[String] = None,
                tag: Option[String] = None): Task[List[Workflow]] = collection.transaction { txn =>
    txn.query
      .filter(_.finished === false)
      .stream
      .filter { wf =>
        name.forall(_ == wf.name) &&
        tag.forall(wf.tags.contains)
      }
      .toList
  }.flatMap { workflows =>
    Task.sequence(workflows.map(wf => cancel(wf._id)))
  }

  def waitForStatus(workflowId: Id[Workflow],
                    status: WorkflowStatus,
                    allowForLaterStatuses: Boolean = true,
                    delay: FiniteDuration = 500.millis): Task[Workflow] = collection.transaction { txn =>
    txn.get(workflowId).flatMap {
      case Some(workflow) => if (workflow.status == status || (allowForLaterStatuses && workflow.status >= status)) {
        Task.pure(workflow)
      } else {
        Task.sleep(delay).flatMap(_ => waitForStatus(workflowId, status, allowForLaterStatuses, delay))
      }
      case None => Task.error(new RuntimeException(s"Workflow not found by id: $workflowId"))
    }
  }

  def waitForFinished(workflowId: Id[Workflow], delay: FiniteDuration = 500.millis): Task[Workflow] = collection.transaction { txn =>
    txn.get(workflowId).flatMap {
      case Some(workflow) => if (workflow.finished) {
        Task.pure(workflow)
      } else {
        Task.sleep(delay).flatMap(_ => waitForFinished(workflowId, delay))
      }
      case None => Task.error(new RuntimeException(s"Workflow not found by id: $workflowId"))
    }
  }

  private def executeWorkflow(workflow: Workflow, txn: Transaction[Workflow, WorkflowModel]): Task[Unit] = if (!workflow.finished) {
    for {
      started <- if (!workflow.started) addHistory(workflow._id, WorkflowActivity.Starting, txn) else Task.pure(workflow)
      wf <- recurseWorkflow(started, txn)
      _ <- addHistory(workflow._id, WorkflowActivity.Completed(true), txn).when(!wf.finished && wf.waitingStepId.isEmpty)
      final_ <- txn.get(workflow._id).map(_.get)
      _ <- if (final_.finished && final_.status == WorkflowStatus.Success) onWorkflowCompleted(final_)
           else if (final_.finished && final_.status == WorkflowStatus.Failure) onWorkflowFailed(final_)
           else Task.unit
    } yield ()
  } else {
    logger.warn(s"${workflow.name} submitted to execute, but is already finished")
  }

  private def recurseWorkflow(workflow: Workflow,
                              txn: Transaction[Workflow, WorkflowModel]): Task[Workflow] = if (workflow.queue.nonEmpty && !workflow.finished && workflow.waitingStepId.isEmpty) {
    // Check workflow-level timeout
    val timedOut = for {
      timeout <- workflow.workflowTimeoutMs
      startHistory <- workflow.history.find(_.activity == WorkflowActivity.Starting)
    } yield System.currentTimeMillis() - startHistory.created >= timeout

    if (timedOut.contains(true)) {
      return modify(workflow._id, txn) { wf =>
        Task.pure(wf.copy(
          runningId = None,
          history =
            WorkflowHistory(WorkflowActivity.Completed(false)) ::
              WorkflowHistory(WorkflowActivity.TimedOut(None)) :: wf.history
        ))
      }
    }
    val stepId = workflow.queue.head
    val w = workflow.copy(
      queue = workflow.queue.tail,
      runningId = Some(stepId),
      modified = Timestamp()
    )
    txn.upsert(w).flatMap { workflow =>
      val step = workflow.running.getOrElse(throw UserException(s"Unable to find running step ${workflow.runningId} in workflow ${workflow.name}"))
      val startedHook =
        if (workflow.completed.isEmpty) onWorkflowStarted(workflow).handleError(_ => Task.unit)
        else Task.unit
      startedHook.flatMap(_ => onStepStarted(workflow, stepId).handleError(_ => Task.unit)).flatMap { _ =>
        step match {
          case job: Job[?] => executeJob(workflow, job, txn)
          case trigger: Trigger => enterTriggerWait(workflow, trigger, txn)
          case condition: Condition => evaluateCondition(workflow, condition, txn)
          case approval: Approval => enterApprovalWait(workflow, approval, txn)
          case parallel: Parallel => executeParallel(workflow, parallel, txn)
          case loop: Loop => executeLoop(workflow, loop, txn)
          case sub: SubWorkflow => executeSubWorkflow(workflow, sub, txn)
          case _: Recycle => executeRecycle(workflow, txn)
          case _ => Task.error(new UnsupportedOperationException(s"Step type ${step.getClass.getSimpleName} not supported"))
        }
      }
    }
  } else {
    Task.pure(workflow)
  }

  private def executeJob(workflow: Workflow,
                         job: Job[?],
                         txn: Transaction[Workflow, WorkflowModel],
                         attempt: Int = 0): Task[Workflow] = {
    val p = Var(Progress(None))
    val pm = ProgressManager.timeDelayed(5.seconds, ProgressManager(p))
    val total = workflow.steps.map(_.weight).sum
    val current = workflow.completed.flatMap(workflow.byStepId).map(_.weight).sum
    // Per-job activity flag. The `timeDelayed` ProgressManager schedules
    // emits up to 5s out — if a fast job completes before the scheduled
    // emit fires, the emit would otherwise leak the previous job's message
    // into the global `WorkflowProgress` Var (the next job's name still
    // shows correctly because it comes from `runningId`, but the message
    // would be the stale one). Listener checks the flag and short-circuits
    // once `jobActive = false`. Set `false` after the deliberate
    // `p @= Progress(Some(1.0))` below so the final 100% update lands.
    @volatile var jobActive: Boolean = true
    val reaction = p.attachAndFire { p =>
      try {
        if (!jobActive) () else {
          val pv = p.value.map(v => math.max(math.min(v, 1.0), 0.0))
          val workflowProgress = (current + (pv.getOrElse(0.0) * job.weight)) / total
          val update = ProgressUpdate(
            workflow = workflow,
            step = job,
            stepProgress = pv,
            workflowProgress = workflowProgress,
            message = p.message
          )
          WorkflowProgress @= Some(update)
        }
      } catch {
        case t: Throwable => scribe.error(t)
      }
    }
    def detachReaction(): Unit = {
      jobActive = false
      try { p.reactions -= reaction; () } catch { case t: Throwable => scribe.error(t) }
    }
    val startTime = System.currentTimeMillis()
    addHistory(workflow._id, WorkflowActivity.StepStarted(job.id), txn).flatMap { workflow =>
      p @= Progress()
      val ctx: JobContext = new JobContext {
        override def updateStepsInTxn(workflowId: Id[Workflow], newSteps: List[Step]): Task[Workflow] =
          updateStepsIn(txn, workflowId, newSteps)
      }
      val baseTask = job.executeToJsonContextualized(workflow, pm, ctx)
      val executionTask = job.executionTimeoutMs match {
        case Some(timeout) =>
          Task.defer {
            val latch = new java.util.concurrent.CountDownLatch(1)
            @volatile var result: Either[Throwable, Json] = Left(new java.util.concurrent.TimeoutException(s"Step ${job.name} timed out after ${timeout}ms"))
            val worker = Thread.startVirtualThread { () =>
              try {
                result = Right(baseTask.sync())
              } catch {
                case _: InterruptedException =>
                  result = Left(new java.util.concurrent.TimeoutException(s"Step ${job.name} timed out after ${timeout}ms"))
                case t: Throwable =>
                  result = Left(t)
              } finally {
                latch.countDown()
              }
            }
            if (!latch.await(timeout, java.util.concurrent.TimeUnit.MILLISECONDS)) {
              worker.interrupt()
              latch.await() // Wait for clean shutdown after interrupt
            }
            result match {
              case Right(json) => Task.pure(json)
              case Left(t) => Task.error(t)
            }
          }
        case None => baseTask
      }
      executionTask.flatMap { payload =>
        p @= Progress(Some(1.0))
        detachReaction()
        val durationMs = System.currentTimeMillis() - startTime
        modify(workflow._id, txn) { workflow =>
          val result = StepResult(job.id, job.name, StepResultStatus.Completed, output = Some(payload), durationMs = durationMs)
          Task.pure(addStepResult(workflow, result).copy(
            runningId = None,
            completed = job.id :: workflow.completed,
            payloads = workflow.payloads + (job.id -> payload),
            history = WorkflowHistory(WorkflowActivity.StepSuccess(job.id)) :: workflow.history,
          ))
        }.flatTap(wf => onStepCompleted(wf, job.id, success = true))
      }.handleError { throwable =>
        if (attempt < job.retryCount) {
          val delay = job.retryBackoff.delayForAttempt(job.retryDelayMs, attempt)
          logger.warn(s"Step ${job.name} failed (attempt ${attempt + 1}/${job.retryCount}), retrying in ${delay}ms...").flatMap { _ =>
            p @= Progress(None)
            detachReaction()
            addHistory(workflow._id, WorkflowActivity.StepRetrying(job.id, attempt + 1, job.retryCount), txn).flatMap { wf =>
              Task.sleep(delay.millis).flatMap { _ =>
                executeJob(wf, job, txn, attempt + 1)
              }
            }
          }
        } else if (job.continueOnError) {
          logger.error(s"Step ${job.name} failed, continuing", throwable).flatMap { _ =>
            p @= Progress(Some(1.0))
            detachReaction()
            val durationMs = System.currentTimeMillis() - startTime
            modify(workflow._id, txn) { workflow =>
              val result = StepResult(job.id, job.name, StepResultStatus.Failed, durationMs = durationMs, error = Some(throwable.getMessage))
              Task.pure(addStepResult(workflow, result).copy(
                runningId = None,
                completed = job.id :: workflow.completed,
                history = WorkflowHistory(WorkflowActivity.StepFailure(job.id, s"${job.name} failed with ${throwable.getMessage}")) :: workflow.history,
              ))
            }.flatTap(wf => onStepCompleted(wf, job.id, success = false))
          }
        } else {
          logger.error(s"Step ${job.name} failed", throwable).flatMap { _ =>
            p @= Progress(Some(1.0))
            detachReaction()
            val durationMs = System.currentTimeMillis() - startTime
            modify(workflow._id, txn) { workflow =>
              val result = StepResult(job.id, job.name, StepResultStatus.Failed, durationMs = durationMs, error = Some(throwable.getMessage))
              Task.pure(addStepResult(workflow, result).copy(
                runningId = None,
                completed = job.id :: workflow.completed,
                history =
                  WorkflowHistory(WorkflowActivity.Completed(false)) ::
                    WorkflowHistory(WorkflowActivity.StepFailure(job.id, s"${job.name} failed with ${throwable.getMessage}")) ::
                    workflow.history,
              ))
            }.flatTap(wf => onStepCompleted(wf, job.id, success = false))
          }
        }
      }
    }.flatMap { workflow =>
      recurseWorkflow(workflow, txn)
    }
  }

  private def enterTriggerWait(workflow: Workflow,
                                trigger: Trigger,
                                txn: Transaction[Workflow, WorkflowModel]): Task[Workflow] = {
    for {
      regPayload <- trigger.register(workflow)
      _ <- addHistory(workflow._id, WorkflowActivity.WaitingOnTrigger(trigger.id), txn)
      wf <- modify(workflow._id, txn) { wf =>
        Task.pure(wf.copy(
          runningId = None,
          waitingStepId = Some(trigger.id),
          waitingSince = Some(System.currentTimeMillis()),
          payloads = wf.payloads + (trigger.id -> regPayload)
        ))
      }
    } yield wf
  }

  private def enterApprovalWait(workflow: Workflow,
                                 approval: Approval,
                                 txn: Transaction[Workflow, WorkflowModel]): Task[Workflow] = {
    for {
      _ <- approval.onWaiting(workflow)
      _ <- addHistory(workflow._id, WorkflowActivity.WaitingOnApproval(approval.id), txn)
      wf <- modify(workflow._id, txn) { wf =>
        Task.pure(wf.copy(
          runningId = None,
          waitingStepId = Some(approval.id),
          waitingSince = Some(System.currentTimeMillis())
        ))
      }
    } yield wf
  }

  private def evaluateCondition(workflow: Workflow,
                                 condition: Condition,
                                 txn: Transaction[Workflow, WorkflowModel]): Task[Workflow] = {
    addHistory(workflow._id, WorkflowActivity.StepStarted(condition.id), txn).flatMap { wf =>
      condition.evaluate(wf).flatMap { result =>
        val targetId = if (result) condition.onTrue else condition.onFalse
        for {
          _ <- addHistory(wf._id, WorkflowActivity.BranchTaken(condition.id, targetId), txn)
          updated <- modify(wf._id, txn) { w =>
            // Find the target step's position in the queue or step list and jump to it
            Task.pure(w.copy(
              runningId = None,
              completed = condition.id :: w.completed,
              queue = targetId :: w.queue
            ))
          }
          result <- recurseWorkflow(updated, txn)
        } yield result
      }
    }
  }

  private def executeParallel(workflow: Workflow,
                               parallel: Parallel,
                               txn: Transaction[Workflow, WorkflowModel]): Task[Workflow] = {
    addHistory(workflow._id, WorkflowActivity.StepStarted(parallel.id), txn).flatMap { wf =>
      val branchTasks = parallel.branches.map { branchStepIds =>
        executeBranch(wf, branchStepIds, txn)
      }
      val joinTask: Task[List[Workflow]] = parallel.joinMode match {
        case JoinMode.All => Task.sequence(branchTasks)
        case JoinMode.Any =>
          // Race all branches — return the first to complete
          Task.defer {
            val result = new java.util.concurrent.atomic.AtomicReference[Option[Workflow]](None)
            val latch = new java.util.concurrent.CountDownLatch(1)
            branchTasks.foreach { task =>
              Task {
                val wf = task.sync()
                if (result.compareAndSet(None, Some(wf))) {
                  latch.countDown()
                }
              }.start()
            }
            latch.await()
            Task.pure(List(result.get.get))
          }
      }
      joinTask.flatMap { results =>
        val mergedPayloads = results.flatMap(_.payloads).toMap
        modify(wf._id, txn) { w =>
          Task.pure(w.copy(
            runningId = None,
            completed = parallel.id :: w.completed,
            payloads = w.payloads ++ mergedPayloads,
            history = WorkflowHistory(WorkflowActivity.StepSuccess(parallel.id)) :: w.history
          ))
        }
      }.flatMap(recurseWorkflow(_, txn))
    }
  }

  private def executeBranch(workflow: Workflow,
                             stepIds: List[Id[Step]],
                             txn: Transaction[Workflow, WorkflowModel]): Task[Workflow] = {
    // Execute steps in the branch sequentially
    val branchWorkflow = workflow.copy(queue = stepIds, runningId = None)
    stepIds.foldLeft(Task.pure(branchWorkflow)) { (wfTask, stepId) =>
      wfTask.flatMap { wf =>
        wf.byStepId(stepId) match {
          case Some(job: Job[?]) =>
            val p = Var(Progress(None))
            val pm = ProgressManager(p)
            job.executeToJson(wf, pm).map { payload =>
              wf.copy(
                completed = stepId :: wf.completed,
                payloads = wf.payloads + (stepId -> payload)
              )
            }
          case _ => Task.pure(wf)
        }
      }
    }
  }

  private def executeLoop(workflow: Workflow,
                           loop: Loop,
                           txn: Transaction[Workflow, WorkflowModel]): Task[Workflow] = {
    addHistory(workflow._id, WorkflowActivity.StepStarted(loop.id), txn).flatMap { wf =>
      val items = wf.variables.get(loop.itemsVariable) match {
        case Some(json) => json.asVector
        case None => Vector.empty
      }
      // Save variables that will be overwritten by loop iteration (for scope isolation)
      val savedVarNames = Set(loop.itemVariableName, s"${loop.itemVariableName}_index", s"${loop.itemVariableName}_total")
      val savedVars = wf.variables.filter { case (k, _) => savedVarNames.contains(k) }

      val resultsTask = items.zipWithIndex.foldLeft(Task.pure((wf, Vector.empty[Json]))) { case (acc, (item, index)) =>
        acc.flatMap { case (currentWf, results) =>
          addHistory(currentWf._id, WorkflowActivity.LoopIteration(loop.id, index), txn).flatMap { iterWf =>
            val iterVars = iterWf.variables +
              (loop.itemVariableName -> item) +
              (s"${loop.itemVariableName}_index" -> fabric.num(index)) +
              (s"${loop.itemVariableName}_total" -> fabric.num(items.size))
            val iterWorkflow = modify(iterWf._id, txn)(w => Task.pure(w.copy(variables = iterVars)))
            iterWorkflow.flatMap { w =>
              executeBranch(w, loop.bodySteps, txn).map { branchResult =>
                val output = branchResult.completed.headOption.flatMap(branchResult.payloads.get).getOrElse(Null)
                (branchResult, results :+ output)
              }
            }
          }
        }
      }
      resultsTask.flatMap { case (loopWf, results) =>
        val resultJson = fabric.arr(results: _*)
        modify(loopWf._id, txn) { w =>
          // Restore saved variables and remove loop-scoped ones, then add results
          val restoredVars = (w.variables -- savedVarNames) ++ savedVars + (loop.outputVariable -> resultJson)
          Task.pure(w.copy(
            runningId = None,
            completed = loop.id :: w.completed,
            payloads = w.payloads + (loop.id -> resultJson),
            variables = restoredVars,
            history = WorkflowHistory(WorkflowActivity.StepSuccess(loop.id)) :: w.history
          ))
        }
      }.flatMap(recurseWorkflow(_, txn))
    }
  }

  private def executeSubWorkflow(parentWorkflow: Workflow,
                                  sub: SubWorkflow,
                                  txn: Transaction[Workflow, WorkflowModel]): Task[Workflow] = {
    implicit val sRW: RW[Step] = collection.model.stepRW
    val resolvedSteps = sub.childSteps
    val child = Workflow(
      name = sub.childName,
      steps = resolvedSteps,
      scheduled = System.currentTimeMillis(),
      queue = resolvedSteps.map(_.id),
      sourceId = parentWorkflow.sourceId,
      variables = sub.inputVariables,
      parentRunId = Some(parentWorkflow._id),
      history = List(
        WorkflowHistory(WorkflowActivity.Scheduled(System.currentTimeMillis())),
        WorkflowHistory(WorkflowActivity.Created)
      )
    )
    for {
      _ <- addHistory(parentWorkflow._id, WorkflowActivity.ChildWorkflowStarted(sub.id, child._id), txn)
      inserted <- txn.insert(child)
      // Execute child inline to avoid deadlock with maxConcurrentWorkflows
      _ <- executeWorkflow(inserted, txn)
      finished <- txn.get(child._id).map(_.get)
      _ <- addHistory(parentWorkflow._id, WorkflowActivity.ChildWorkflowCompleted(sub.id, child._id, finished.status == WorkflowStatus.Success), txn)
      wf <- modify(parentWorkflow._id, txn) { w =>
        val childPayload = finished.payload.getOrElse(Null)
        Task.pure(w.copy(
          runningId = None,
          completed = sub.id :: w.completed,
          payloads = w.payloads + (sub.id -> childPayload),
          history = WorkflowHistory(WorkflowActivity.StepSuccess(sub.id)) :: w.history
        ))
      }
      result <- recurseWorkflow(wf, txn)
    } yield result
  }

  private def executeRecycle(workflow: Workflow,
                              txn: Transaction[Workflow, WorkflowModel]): Task[Workflow] = {
    val recycle = workflow.running.collect { case r: Recycle => r }
    val currentCount = workflow.variables.get("_recycleCount").flatMap(j => scala.util.Try(j.asInt).toOption).getOrElse(0) + 1
    val maxExec = recycle.map(_.maxExecutions).getOrElse(0)

    // Check if we've reached the execution limit
    if (maxExec > 0 && currentCount >= maxExec) {
      scribe.info(s"[WORKFLOW] Recycle limit reached ($currentCount/$maxExec) for ${workflow.name}")
      // Don't recycle — complete the workflow instead
      for {
        _ <- addHistory(workflow._id, WorkflowActivity.Completed(success = true), txn)
        wf <- modify(workflow._id, txn) { wf =>
          Task.pure(wf.copy(
            runningId = None,
            completed = workflow.runningId.toList ::: wf.completed
          ))
        }
      } yield wf
    } else {
      // Resolve parent to get fresh steps if available
      resolveParent(workflow.sourceId).flatMap { parentOpt =>
        val freshSteps = parentOpt.map(_.workflow.steps).getOrElse(workflow.steps)
        // If parent has triggers, ensure Recycle step is preserved
        val stepsWithRecycle = if (freshSteps.exists(_.isInstanceOf[Recycle])) {
          freshSteps
        } else {
          recycle.map(r => freshSteps :+ r).getOrElse(freshSteps)
        }
        // Reset to only the default variable values from the definitions, but carry _recycleCount
        val defaultVars = workflow.variableDefs.flatMap(vd => vd.defaultValue.map(vd.name -> _)).toMap
        val fresh = Workflow(
          name = workflow.name,
          steps = stepsWithRecycle,
          scheduled = System.currentTimeMillis(),
          queue = stepsWithRecycle.map(_.id),
          sourceId = workflow.sourceId,
          variableDefs = workflow.variableDefs,
          variables = defaultVars + ("_recycleCount" -> fabric.num(currentCount)),
          priority = workflow.priority,
          parentRunId = workflow.parentRunId,
          history = List(
            WorkflowHistory(WorkflowActivity.Scheduled(System.currentTimeMillis())),
            WorkflowHistory(WorkflowActivity.Created)
          )
        )
        collection.transaction(_.insert(fresh)).flatMap { _ =>
          changed @= true
          // Continue the current workflow (Recycle step is done, workflow proceeds to completion)
          modify(workflow._id, txn) { wf =>
            Task.pure(wf.copy(
              runningId = None,
              completed = workflow.runningId.toList ::: wf.completed
            ))
          }
        }.flatMap(recurseWorkflow(_, txn))
      }
    }
  }

  def modify(workflowId: Id[Workflow], transaction: Transaction[Workflow, WorkflowModel])
            (f: Workflow => Task[Workflow]): Task[Workflow] = transaction.modify(workflowId) {
    case Some(workflow) => f(workflow).map { w =>
      Some(w.copy(modified = Timestamp()))
    }
    case None => logger.error(s"Workflow not found $workflowId").map(_ => None)
  }.flatMap { w =>
    transaction.commit.map(_ => w.get)
  }

  def addHistory(workflowId: Id[Workflow],
                 activity: WorkflowActivity,
                 transaction: Transaction[Workflow, WorkflowModel]): Task[Workflow] = modify(workflowId, transaction) { workflow =>
    Task.pure(workflow.copy(history = WorkflowHistory(activity) :: workflow.history))
  }

  private def addStepResult(workflow: Workflow, result: StepResult): Workflow = {
    workflow.copy(stepResults = result :: workflow.stepResults)
  }

  private def unregisterIfTrigger(workflow: Workflow, stepId: Id[Step]): Task[Unit] = {
    workflow.byStepId(stepId) match {
      case Some(trigger: Trigger) => trigger.unregister(workflow)
      case _ => Task.unit
    }
  }

  /** Find all non-finished workflow instances spawned from a given parent. */
  def activeBySourceId(sourceId: Id[WorkflowParent]): Task[List[Workflow]] =
    collection.transaction(_.query.filter(w => w.finished === false && w.sourceId === sourceId).toList)

  /** Update the steps of a running workflow instance in place.
    * Recomputes the queue to include only steps not yet completed. */
  def updateSteps(workflowId: Id[Workflow], newSteps: List[Step]): Task[Workflow] =
    collection.transaction { txn =>
      updateStepsIn(txn, workflowId, newSteps)
    }

  /** Same as [[updateSteps]] but uses the supplied transaction
    * instead of opening a fresh one. Lets a job's execute body
    * mutate the workflow's queue WITHIN the runner's own
    * transaction so the new queue is visible to the runner's
    * post-execute modify (which would otherwise read its txn-cached
    * pre-update snapshot and clobber the change).
    *
    * Filters both `completed` AND `runningId` out of the new
    * queue: when called from inside a job's execute the running
    * step isn't yet in `completed`, but it's also already running —
    * including its id in the queue would cause Strider's runner to
    * pop and re-run it after settle. */
  def updateStepsIn(txn: Transaction[Workflow, WorkflowModel],
                    workflowId: Id[Workflow],
                    newSteps: List[Step]): Task[Workflow] =
    modify(workflowId, txn) { wf =>
      val excluded = (wf.completed ++ wf.runningId.toList).toSet
      val newQueue = newSteps.map(_.id).filterNot(excluded.contains)
      Task.pure(wf.copy(steps = newSteps, queue = newQueue))
    }

  /** Propagate step changes from a parent to all active workflow instances.
    * Returns which instances were updated in-place and which need user decision. */
  def propagateChanges(sourceId: Id[WorkflowParent], newSteps: List[Step])(implicit stepRW: RW[Step]): Task[PropagationResult] = {
    activeBySourceId(sourceId).flatMap { activeRuns =>
      val results = activeRuns.map { wf =>
        if (wf.runningId.isDefined) {
          // Currently executing a step — skip, will pick up changes on next recycle
          Task.pure((0, Option.empty[Id[Workflow]]))
        } else if (wf.completed.isEmpty) {
          // Hasn't started any steps yet — safe to update all
          updateSteps(wf._id, newSteps).map(_ => (1, Option.empty[Id[Workflow]]))
        } else {
          // Has completed steps — check if any completed steps changed
          val completedSteps = wf.completed.flatMap(id => wf.steps.find(_.id == id))
          val newStepsByPosition = newSteps.take(completedSteps.size)
          val allMatch = completedSteps.zip(newStepsByPosition).forall { case (old, updated) =>
            StepFingerprint.matches(old, updated)
          }
          if (allMatch) {
            // Completed steps unchanged — safe to update remaining
            updateSteps(wf._id, newSteps).map(_ => (1, Option.empty[Id[Workflow]]))
          } else {
            // Completed steps changed — needs user decision
            Task.pure((0, Some(wf._id)))
          }
        }
      }
      Task.sequence(results).map { pairs =>
        PropagationResult(
          updatedInPlace = pairs.map(_._1).sum,
          needsDecision = pairs.flatMap(_._2)
        )
      }
    }
  }

  /** Resolve a conflict for workflow runs where completed steps were changed. */
  def resolveConflict(runIds: List[Id[Workflow]], action: ConflictAction, sourceId: Id[WorkflowParent]): Task[Unit] = {
    action match {
      case ConflictAction.Stop =>
        Task.sequence(runIds.map(cancel)).unit
      case ConflictAction.Restart =>
        resolveParent(sourceId).flatMap {
          case Some(parent) =>
            val steps = parent.workflow.steps
            Task.sequence(runIds.map(cancel)).flatMap { _ =>
              schedule(parent.workflow.name, steps, sourceId).unit
            }
          case None =>
            Task.sequence(runIds.map(cancel)).unit
        }
      case ConflictAction.ApplyRemaining =>
        resolveParent(sourceId).flatMap {
          case Some(parent) =>
            Task.sequence(runIds.map(id => updateSteps(id, parent.workflow.steps))).unit
          case None => Task.unit
        }
    }
  }

  def dispose(): Task[Unit] = {
    keepAlive = false
    activeCount.set(0)
    if (monitorFiber != null) {
      monitorFiber.cancel.unit
    } else {
      Task.unit
    }
  }
}
