package strider.step

import fabric.rw._
import lightdb.id.Id

case class Parallel(branches: List[List[Id[Step]]],
                    joinMode: JoinMode = JoinMode.All,
                    id: Id[Step] = Step.id()) extends Step derives RW
