package pv.http.service.config

case class HttpServiceConfig(host: String,
                             port: Int,
                             bootstrapServers: String,
                             topic: String
                            )