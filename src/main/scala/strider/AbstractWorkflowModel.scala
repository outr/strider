package strider

import strider.step.*

import fabric.Json
import fabric.rw._
import lightdb.id.Id
import lightdb.doc.{JsonConversion, RecordDocumentModel}

abstract class AbstractWorkflowModel extends RecordDocumentModel[Workflow] with JsonConversion[Workflow] {
  implicit def stepRW: RW[Step]

  @deprecated("Use stepRW", "2.0")
  def jobRW: RW[Step] = stepRW

  val name: I[String] = field.index("name", _.name)
  val steps: F[List[Step]] = field("steps", _.steps)
  val variableDefs: F[List[WorkflowVariable]] = field("variableDefs", _.variableDefs)
  val variables: F[Map[String, Json]] = field("variables", _.variables)
  val tags: I[Set[String]] = field.index("tags", _.tags)
  val workflowTimeoutMs: F[Option[Long]] = field("workflowTimeoutMs", _.workflowTimeoutMs)
  val scheduled: I[Long] = field.index("scheduled", _.scheduled)
  val queue: F[List[Id[Step]]] = field("queue", _.queue)
  val priority: I[Double] = field.index("priority", _.priority)
  val runningId: I[Option[Id[Step]]] = field.index("runningId", _.runningId)
  val waitingStepId: I[Option[Id[Step]]] = field.index("waitingStepId", _.waitingStepId)
  val waitingSince: F[Option[Long]] = field("waitingSince", _.waitingSince)
  val completed: F[List[Id[Step]]] = field("completed", _.completed)
  val payloads: F[Map[Id[Step], Json]] = field("payloads", _.payloads)
  val stepResults: F[List[StepResult]] = field("stepResults", _.stepResults)
  val history: F[List[WorkflowHistory]] = field("history", _.history)
  val sourceId: I[Id[WorkflowParent]] = field.index("sourceId", _.sourceId)
  val parentRunId: I[Option[Id[Workflow]]] = field.index("parentRunId", _.parentRunId)
  val finished: I[Boolean] = field.index("finished", _.finished)

  override implicit val rw: RW[Workflow] = RW.gen[Workflow].withPreWrite { json =>
    import fabric.*
    import fabric.dsl.*
    var j = json

    // Migrate "jobs" → "steps"
    j.get("jobs") match {
      case Some(jobs) if j.get("steps").isEmpty =>
        j = j.merge(obj("steps" -> jobs))
        j = j.remove(fabric.JsonPath.parse("jobs"))
      case _ =>
    }

    // Migrate history activity types: JobStarted→StepStarted, JobSuccess→StepSuccess, JobFailure→StepFailure
    j.get("history").foreach { history =>
      val migrated = history.asVector.map { entry =>
        entry.get("activity").map { activity =>
          val actType = activity.get("type").map(_.asString).getOrElse("")
          val migratedActivity = actType match {
            case "JobStarted" =>
              val stepId = activity.get("jobId").getOrElse(Null)
              obj("stepId" -> stepId, "type" -> str("StepStarted"))
            case "JobSuccess" =>
              val stepId = activity.get("jobId").getOrElse(Null)
              obj("stepId" -> stepId, "type" -> str("StepSuccess"))
            case "JobFailure" =>
              val stepId = activity.get("jobId").getOrElse(Null)
              val errorMessage = activity.get("errorMessage").getOrElse(str(""))
              obj("stepId" -> stepId, "errorMessage" -> errorMessage, "type" -> str("StepFailure"))
            case _ => activity
          }
          entry.merge(obj("activity" -> migratedActivity))
        }.getOrElse(entry)
      }
      j = j.merge(obj("history" -> Arr(migrated, None)))
    }

    // Add default sourceId if missing
    if (j.get("sourceId").isEmpty) {
      j = j.merge(obj("sourceId" -> str("legacy")))
    }

    // Clear malformed legacy payloads (arrays with wrong shape from prior migrations).
    // Obj {"stepId": value} is the valid current format for Map[Id[Step], Json], leave it alone.
    j.get("payloads").foreach { payloads =>
      payloads match {
        case a: Arr if a.value.isEmpty =>
          j = j.merge(obj("payloads" -> obj()))
        case a: Arr if a.value.nonEmpty => a.value.head match {
          case o: Obj if o.value.contains("key") && o.value.contains("value") =>
            // Legacy key-value pair format: [{"key": "stepId", "value": ...}, ...] → {"stepId": ...}
            val converted = obj(a.value.collect {
              case kv: Obj =>
                val k = kv.get("key").map(_.asString).getOrElse("")
                val v = kv.get("value").getOrElse(Null)
                k -> v
            }.filter(_._1.nonEmpty)*)
            j = j.merge(obj("payloads" -> converted))
          case _: Arr => j = j.merge(obj("payloads" -> obj())) // Malformed array-of-arrays
          case _: Str => j = j.merge(obj("payloads" -> obj())) // Malformed flat array
          case _ => j = j.merge(obj("payloads" -> obj())) // Unknown array format
        }
        case _ => // Obj format is correct, leave as-is
      }
    }

    // Add default stepResults if missing
    if (j.get("stepResults").isEmpty) {
      j = j.merge(obj("stepResults" -> Arr(Vector.empty, None)))
    }

    j
  }

  implicit val previewRW: RW[WorkflowPreview] = RW.gen
}
