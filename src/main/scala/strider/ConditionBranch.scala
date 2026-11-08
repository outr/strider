package strider

import fabric.rw.*

/** Which branch of a condition was taken: `Then` when it evaluated true (its `onTrue`), `Else` when false (`onFalse`). */
enum ConditionBranch derives RW {
  case Then, Else
}
