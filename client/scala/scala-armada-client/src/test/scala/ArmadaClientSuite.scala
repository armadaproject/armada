import api.health.HealthCheckResponse.ServingStatus
import api.submit.Queue
import io.armadaproject.armada.Client
import scala.util.Random

class ArmadaClientSuite extends munit.FunSuite {
  val host = "localhost"
  val port = 30002

  test("test Armada cluster health") {
    val ac = new Client(host, port)
    val healthStatus = ac.getHealth()
    assertEquals(healthStatus, ServingStatus.SERVING)
  }

  test("test queue existence, creation, deletion") {
    val ac = new Client(host, port)
    val qName = "test-queue-" + Random.alphanumeric.take(8).mkString
    var q: Queue = new Queue()

    // queue should not exist yet
    intercept[io.grpc.StatusRuntimeException] {
      q = ac.getQueue(qName)
    }
    assertNotEquals(q.name, qName)

    ac.createQueue(qName)
    q = ac.getQueue(qName)
    assertEquals(q.name, qName)

    ac.deleteQueue(qName)
    q = new Queue()
    intercept[io.grpc.StatusRuntimeException] {
      q = ac.getQueue(qName)
    }
    assertNotEquals(q.name, qName)
  }
}
