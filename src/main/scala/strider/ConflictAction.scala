package strider

import fabric.rw.*

enum ConflictAction {
  case Stop, Restart, ApplyRemaining
}

object ConflictAction {
  given RW[ConflictAction] = RW.enumeration(ConflictAction.values.toList)
}
