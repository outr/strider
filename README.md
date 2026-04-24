# Strider

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

**Strider** is a lightweight, embedded workflow engine for Scala 3 on the JVM.

Define workflows as a list of typed steps — jobs, triggers, conditions, approvals, parallel forks, loops, and sub-workflows — and let Strider handle scheduling, retries, persistence, and crash recovery. No external server required; Strider runs in-process and persists state through [LightDB](https://github.com/outr/lightdb).

## Features

- **Step-based programming model** — express workflows as first-class Scala types (`case class`/`trait`), not XML or a DSL
- **Triggers** — pause workflows and resume on external events. Triggers can either `Continue` (resume the same workflow) or `Branch` (clone the workflow at its current state on every firing — ideal for recurring schedules)
- **Conditions** — branch execution based on workflow state
- **Approvals** — pause for human decisions with timeouts
- **Parallel** — fork into multiple branches and join on all or any
- **Loops** — iterate body steps over a variable list with scoped iteration variables
- **Sub-workflows** — execute child workflows inline
- **Recycle** — reset a completed workflow to run again (useful for "once-at-a-time" recurring jobs)
- **Retries with backoff** — configurable per-step with Fixed/Exponential/Linear strategies
- **Continue on error** — let specific steps fail without failing the whole workflow
- **Workflow-level timeouts** — bound total execution time
- **Step-level timeouts** — bound individual step execution via cooperative cancellation
- **Lifecycle hooks** — `onWorkflowCompleted`, `onWorkflowFailed`, `onStepCompleted` for observability
- **Tags & listing** — tag workflows for categorization; query by name/tag/status
- **Deduplication** — prevent duplicate scheduling with `scheduleIfNotActive`
- **Bulk operations** — `cancelAll` by name/tag
- **Configurable concurrency** — `maxConcurrentWorkflows` controls parallel execution
- **Crash recovery** — running and waiting workflows are safely marked failed on restart
- **REST services** — optional status/list/resume/cancel endpoints built on [spice](https://github.com/outr/spice)

## Requirements

- Scala 3.8+
- Java 21+ (uses virtual threads)

## Installation

```scala
libraryDependencies += "com.outr" %% "strider" % "0.1.0-SNAPSHOT"
```

## Quick Start

```scala
import strider._
import strider.step._
import fabric._
import fabric.rw._
import lightdb._
import rapid._

// Define a job step
case class ReverseTextJob(text: String, id: lightdb.id.Id[Step] = Step.id()) extends Job[String] {
  def execute(workflow: Workflow, pm: lightdb.progress.ProgressManager): Task[String] =
    Task(text.reverse)
}

// Wire up the model with your step types
object MyWorkflowModel extends AbstractWorkflowModel {
  override implicit lazy val stepRW: RW[Step] = RW.poly()(
    RW.gen[ReverseTextJob]
    // ... your other step types
  )
}

// Create a manager
object MyWorkflowManager extends AbstractWorkflowManager[WorkflowParent, MyWorkflowModel.type](db.workflows) {
  override protected def resolveParent(sourceId: lightdb.id.Id[WorkflowParent]): Task[Option[WorkflowParent]] =
    Task.pure(None)
}

// Schedule and await
for {
  workflow <- MyWorkflowManager.schedule(
    name = "greet",
    steps = List(ReverseTextJob("Hello, World!")),
    sourceId = someSourceId
  )
  finished <- MyWorkflowManager.waitForFinished(workflow._id)
} yield finished.payload // Some(str("!dlroW ,olleH"))
```

## Status

Strider is extracted from a production workflow system and tested with 66+ scenarios. Version 0.x indicates the API may evolve as it's put through broader use.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
