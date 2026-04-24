package strider

import fabric.rw._

enum VariableType derives RW {
  case Text, Number, Boolean, Json, List
}
