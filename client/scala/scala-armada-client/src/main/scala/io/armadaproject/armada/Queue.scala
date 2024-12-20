package io.armadaproject.armada

import io.grpc.ManagedChannelBuilder
import api.submit.{Queue, QueueGetRequest, SubmitGrpc}

object Queue {
  val host = "localhost"
  val port = 30002

  // TODO: add permissions and other parameters?
  def createQueue(name: String): Unit = {
    val channel =
      ManagedChannelBuilder.forAddress(host, port).usePlaintext().build()
    val blockingStub = SubmitGrpc.blockingStub(channel)
    val q = api.submit.Queue().withName(name)

    blockingStub.createQueue(q)
  }

  def getQueue(name: String): Queue = {
    val qReq = QueueGetRequest(name)
    val channel =
      ManagedChannelBuilder.forAddress(host, port).usePlaintext().build()
    val blockingStub = SubmitGrpc.blockingStub(channel)

    blockingStub.getQueue(qReq)
  }
}
