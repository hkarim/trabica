package trabica.model

trait LogEntry[A] {
  def term(a: A): Term
  def index(a: A): Index
}
