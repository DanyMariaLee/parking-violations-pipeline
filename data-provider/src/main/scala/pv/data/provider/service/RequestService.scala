package pv.data.provider.service

import cats.effect.IO
import com.typesafe.scalalogging.Logger
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.CloseableHttpClient
import pv.common.output.domain.WrappedNYCData
import spray.json.RootJsonFormat

trait RequestService {

  implicit def logger: Logger

  def send(host: String, port: Int, data: WrappedNYCData
          )(implicit
            rj: RootJsonFormat[WrappedNYCData],
            client: CloseableHttpClient): CloseableHttpResponse = {

    val request = buildRequest(host, port, data)

    makeCall(client, request)
      .map { response =>
        logger.info(s"Response: $response")
        response
      }
      .handleErrorWith { e =>
        logger.error(s"Failed to send: $data", e)
        IO.raiseError(e)
      }.unsafeRunSync()
  }

  private[service] def buildRequest(host: String, port: Int, data: WrappedNYCData
                                   )(implicit rj: RootJsonFormat[WrappedNYCData]): HttpPost = {

    val post = new HttpPost(s"http://$host:$port/")
    post.setHeader("Content-type", "application/json")

    val jsonString = rj.write(data).toString()

    post.setEntity(new StringEntity(jsonString))
    post
  }

  private[service] def makeCall(client: CloseableHttpClient,
                                request: HttpPost): IO[CloseableHttpResponse] =
    IO(client.execute(request))
}
