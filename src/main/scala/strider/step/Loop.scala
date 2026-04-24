package strider.step

import fabric.rw._
import lightdb.id.Id

case class Loop(itemsVariable: String,
                bodySteps: List[Id[Step]],
                itemVariableName: String = "item",
                outputVariable: String = "loopResults",
                id: Id[Step] = Step.id()) extends Step derives RW
