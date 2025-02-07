import io.armadaproject.armada.ArmadaClient

object Main {
  def main(args: Array[String]): Unit = {
    val host = "localhost"
    val port = 30002

    val ac = ArmadaClient(host, port)
    val status = ac.EventHealth()
    println(status)
  }
}
