package strider

import fabric.rw._

enum StepResultStatus derives RW {
  case Completed, Failed, Skipped, Waiting, TimedOut
}
