package strider

import fabric.Json
import fabric.rw._

case class WorkflowVariable(name: String,
                            variableType: VariableType = VariableType.Text,
                            description: Option[String] = None,
                            defaultValue: Option[Json] = None,
                            required: Boolean = false) derives RW
