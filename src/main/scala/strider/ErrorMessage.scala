package strider

import fabric.rw._
import spice.ValidationError
import spice.http.HttpStatus

case class ErrorMessage(message: String, code: Option[Int])

object ErrorMessage {
  implicit val rw: RW[ErrorMessage] = RW.gen

  def apply(message: String): ErrorMessage = ErrorMessage(message, None)

  def apply(message: String, status: HttpStatus): ErrorMessage = ErrorMessage(
    message = message,
    code = Some(status.code)
  )

  def apply(errors: List[ValidationError], code: Int): ErrorMessage = ErrorMessage(
    errors
      .map { e =>
        if (e.code != 0) {
          s"${e.code}: ${e.message}"
        } else {
          e.message
        }
      }
      .mkString(", "),
    Some(code)
  )

  def merge(messages: List[ErrorMessage]): Option[ErrorMessage] =
    if (messages.nonEmpty) {
      Some(ErrorMessage(messages.map(_.message).mkString(", "), messages.map(_.code).max))
    } else {
      None
    }
}
