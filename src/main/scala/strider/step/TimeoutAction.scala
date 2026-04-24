package strider.step

import fabric.rw._

enum TimeoutAction derives RW {
  case Proceed, Fail, Skip
}
