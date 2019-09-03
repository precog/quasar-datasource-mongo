/*
 * Copyright 2014–2019 SlamData Inc.
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

final case class TunnelConfig(
    host: String,
    port: Int,
    user: String,
    pass: Option[TunnelConfig.Pass])
    extends Product with Serializable

object TunnelConfig {
  import Pass._
  def sanitize(inp: TunnelConfig): TunnelConfig = {
    val newPass: Option[Pass] = inp.pass map {
      case Password(_) => Password("<REDACTED>")
      case Identity(_, _) => Identity("<REDACTED>", Some("<REDACTED>"))
    }
    inp.copy(pass = newPass)
  }

  implicit val codecTunnelConfig: CodecJson[TunnelConfig] =
    casecodec4(TunnelConfig.apply, TunnelConfig.unapply)("host", "port", "user", "pass")

  trait Pass

  object Pass {
    final case class Identity(prv: String, passphrase: Option[String]) extends Pass
    final case class Password(password: String) extends Pass

    implicit val codecIdentity: CodecJson[Identity] =
      casecodec2(Identity.apply, Identity.unapply)("prv", "passphrase")

    implicit val codecPassword: CodecJson[Password] =
      casecodec1(Password.apply, Password.unapply)("password")

    implicit val codecTunnelPass: CodecJson[Pass] = CodecJson({
      case c: Identity => codecIdentity(c)
      case c: Password => codecPassword(c)
    }, { j: HCursor =>
      val id = codecIdentity(j).map(a => a: Pass)
      id.result.fold(_ => codecPassword(j).map(a => a: Pass), _ => id)
    })
  }
}
