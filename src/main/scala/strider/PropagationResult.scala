package strider

import fabric.rw.*
import lightdb.id.Id

case class PropagationResult(
  updatedInPlace: Int = 0,
  needsDecision: List[Id[Workflow]] = Nil
) derives RW
