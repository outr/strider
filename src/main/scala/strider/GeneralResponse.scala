package strider

trait GeneralResponse {
  def error: Option[ErrorMessage]
}
