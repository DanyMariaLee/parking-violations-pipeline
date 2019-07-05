package pv.data.provider.service

import cats.effect.IO
import com.typesafe.scalalogging.Logger
import okhttp3._
import pv.common.output.domain.WrappedNYCData
import spray.json.RootJsonFormat

trait RequestService {

  implicit def logger: Logger

  def send(host: String, port: Int, data: WrappedNYCData
          )(implicit
            rj: RootJsonFormat[WrappedNYCData],
            client: OkHttpClient): Response = {

    val request = buildRequest(host, port, data)

    makeCall(client, request)
      .map { response =>
        logger.info(s"Response code: ${response.code()}")
        response
      }
      .handleErrorWith { e =>
        logger.error(s"Failed to send: $data", e)
        IO.raiseError(e)
      }.unsafeRunSync()
  }

  private[service] def buildRequest(host: String, port: Int, data: WrappedNYCData
                                   )(implicit rj: RootJsonFormat[WrappedNYCData]): Request = {

    val mediaType = MediaType.get("application/json; charset=utf-8")

    val jsonString = rj.write(data).toString()
    val body = RequestBody.create(mediaType, jsonString)

    new Request.Builder()
      .url(s"http://$host:$port/")
      .post(body)
      .build()
  }

  private[service] def makeCall(client: OkHttpClient, request: Request): IO[Response] =
    IO(client.newCall(request).execute())
}
