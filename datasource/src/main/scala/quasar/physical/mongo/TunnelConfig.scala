/*
 * Copyright 2014–2018 SlamData Inc.
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

final case class TunnelConfig(host: String, port: Int, credentials: Option[TunnelConfig.Credentials])

object TunnelConfig {
  import Credentials._
  def sanitize(inp: TunnelConfig): TunnelConfig = {
    val newCreds: Option[Credentials] = inp.credentials map {
      case LoginPassword(login, _) => LoginPassword(login, "<REDACTED>")
      case Identity(_, _) => Identity("<REDACTED>", Some("<REDACTED>"))
    }
    inp.copy(credentials = newCreds)
  }

  implicit val codecTunnelConfig: CodecJson[TunnelConfig] =
    casecodec3(TunnelConfig.apply, TunnelConfig.unapply)("host", "port", "credentials")

  trait Credentials

  object Credentials {
    final case class Identity(prv: String, passphrase: Option[String]) extends Credentials
    final case class LoginPassword(login: String, password: String) extends Credentials

    implicit val codecIdentity: CodecJson[Identity] =
      casecodec2(Identity.apply, Identity.unapply)("prv", "passphrase")

    implicit val codecLoginPassword: CodecJson[LoginPassword] =
      casecodec2(LoginPassword.apply, LoginPassword.unapply)("login", "password")

    implicit val codecTunnelCredentials: CodecJson[Credentials] = CodecJson({
      case c: Identity => codecIdentity(c)
      case c: LoginPassword => codecLoginPassword(c)
    }, { j: HCursor =>
      val id = codecIdentity(j).map(a => a: Credentials)
      id.result.fold(_ => codecLoginPassword(j).map(a => a: Credentials), _ => id)
    })
  }
}
