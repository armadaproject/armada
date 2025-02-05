import api.event.EventGrpc
import api.health.HealthCheckResponse
import api.submit.Job
import com.google.protobuf.empty.Empty
import io.grpc.ManagedChannelBuilder

object Health {
  def testHealth(args: Array[String]): Unit = {
    val host = "localhost"
    val port = 30002

    val healthStatus = getHealth(host, port)
    println(healthStatus)
  }

  def getHealth(host: String, port: Int): HealthCheckResponse.ServingStatus = {
    val channel =
      ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
    val blockingStub = EventGrpc.blockingStub(channel)

    val reply: HealthCheckResponse = blockingStub.health(Empty())
    return reply.status
  }
}
