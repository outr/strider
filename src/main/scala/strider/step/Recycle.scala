package strider.step

import fabric.rw._
import lightdb.id.Id

/** Recycles (restarts) the workflow from the beginning.
  * @param maxExecutions If > 0, stop recycling after this many total executions. 0 = unlimited. */
case class Recycle(maxExecutions: Int = 0, id: Id[Step] = Step.id()) extends Step derives RW
