import api.event.EventGrpc
import api.health.HealthCheckResponse
import api.submit.Job
import com.google.protobuf.empty
import io.grpc.ManagedChannelBuilder
import queryapi.queryapi.QueryApiGrpc
import queryapi.queryapi.JobStatusResponse

val host = "localhost"
val port = 30002

@main def getArmadaHealth(): Unit =
  val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
  val blockingStub = EventGrpc.blockingStub(channel)

  val reply: HealthCheckResponse = blockingStub.health(empty.Empty())

  println(reply.status)

def getJobStatus(): Unit =
  val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
  val blockingStub = QueryApiGrpc.blockingStub(channel)

  val jsReq = queryapi.queryapi.JobStatusRequest(jobId = "foobar")
  val reply: queryapi.queryapi.JobStatusResponse = blockingStub.getJobStatus(jsReq)

  println(reply)
