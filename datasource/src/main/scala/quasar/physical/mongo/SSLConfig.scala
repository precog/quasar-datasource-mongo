/*
 * Copyright 2020 Precog Data
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.physical.mongo

import slamdata.Predef._

import argonaut._, Argonaut._

final case class SSLConfig(
    serverCA: Option[String],
    clientPEM: Option[String],
    allowInvalidHostnames: Boolean,
    passphrase: Option[String])
    extends Product with Serializable

object SSLConfig {
  def sanitize(inp: SSLConfig): SSLConfig =
    inp.copy(serverCA = Some("<REDACTED>"), clientPEM = Some("<REDACTED>"), passphrase = Some("<REDACTED>"))

  implicit val codecSSLConfig: CodecJson[SSLConfig] =
    casecodec4(SSLConfig.apply, SSLConfig.unapply)("serverCA", "clientPEM", "allowInvalidHostnames", "passphrase")
}
