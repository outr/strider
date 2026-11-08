package strider

import strider.step.Step

import fabric.rw.*
import lightdb.id.Id

/**
 * What a condition step decided: what it tested (the condition's own description of it, when it gives one), the value it
 * evaluated to, the branch that value chose and the step the run went on to.
 */
case class ConditionOutcome(value: Boolean,
                            branch: ConditionBranch,
                            targetStepId: Id[Step],
                            tested: Option[String] = None) derives RW
