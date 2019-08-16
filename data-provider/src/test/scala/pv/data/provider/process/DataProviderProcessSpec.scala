/*
package pv.data.provider.process

import cats.effect.IO
import com.typesafe.scalalogging.Logger
import okhttp3.{OkHttpClient, Response}
import org.scalatest.{FlatSpec, Matchers}
import pv.common.output.domain.WrappedNYCData
import pv.data.provider.DataProviderProcess
import pv.data.provider.TestData.response200
import pv.data.provider.pv.view.config.DataProviderConfig
import spray.json.RootJsonFormat

class DataProviderProcessSpec extends FlatSpec with Matchers {

  behavior of "DataProviderProcess"

  "process" should "return successful Response" in new mocks {

    process.map(_ => assert(true))
      .handleErrorWith { case e =>
        fail("This should succeed")
      }
  }

  "process" should "fail when pv.view.config reader failed" in new mocks {
    override def readConfig: IO[DataProviderConfig] =
      IO.raiseError(new Exception("Failed to read pv.view.config"))

    intercept[Exception](process.unsafeRunSync()).getMessage shouldBe "Failed to read pv.view.config"
  }

  "process" should "fail when file reading failed" in new mocks {
    override def readConfig: IO[DataProviderConfig] =
      IO.raiseError(new RuntimeException("Failed to read csv"))

    intercept[RuntimeException](process.unsafeRunSync()).getMessage shouldBe "Failed to read csv"
  }

  trait mocks extends DataProviderProcess {
    implicit def logger = Logger("mocked")

    implicit val client: OkHttpClient = new OkHttpClient()

    override def send(host: String, port: Int, data: WrappedNYCData
                     )(implicit
                       rj: RootJsonFormat[WrappedNYCData],
                       client: OkHttpClient): Response = response200

  }

}
*/
