package pv.data.provider.service

import cats.effect.IO
import com.typesafe.scalalogging.Logger
import okhttp3._
import org.scalatest.{FlatSpec, Matchers}
import pv.common.json.JsonProtocols
import pv.data.provider.TestData.{wrappedNYC, response200}

class RequestServiceSpec extends FlatSpec with Matchers {

  behavior of "RequestService"

  "send" should "build correct request and run successfully" in new successServiceMock {

    send("localhost", 9000, wrappedNYC).code() shouldBe 200
  }

  "send" should "build correct request and fail" in new failureServiceMock {

    intercept[RuntimeException](
      send("localhost", 9000, wrappedNYC)
    ).getMessage shouldBe "Failed to send"
  }

  trait successServiceMock extends RequestService with JsonProtocols {
    implicit def logger = Logger("mocked")

    implicit val okClient: OkHttpClient = new OkHttpClient()

    override def makeCall(client: OkHttpClient, request: Request): IO[Response] =
      IO(response200)
  }

  trait failureServiceMock extends RequestService with JsonProtocols {

    implicit def logger = Logger("mocked")

    implicit val okClient: OkHttpClient = new OkHttpClient()

    override def makeCall(client: OkHttpClient, request: Request): IO[Response] =
      IO.raiseError(new RuntimeException("Failed to send"))
  }

}
