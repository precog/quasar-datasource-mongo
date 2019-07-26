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

import cats.effect._
import cats.syntax.applicative._
import cats.syntax.functor._
import cats.syntax.flatMap._
import cats.syntax.apply._

import com.jcraft.jsch._
import com.mongodb.connection.ClusterSettings
import com.mongodb.{ConnectionString, ServerAddress}

import quasar.concurrent.BlockingContext

import scalaz.syntax.tag._

import scala.collection.JavaConverters._

object Settings {
  import TunnelConfig._, Pass._

  val SessionName: String = "default"

  def apply[F[_]: Sync: ContextShift](config: MongoConfig, blockingPool: BlockingContext): Resource[F, ClusterSettings] =
    Resource.suspend(connectionString[F](config.connectionString) map { (conn: ConnectionString) =>
      val settings: ClusterSettings = ClusterSettings.builder.applyConnectionString(conn).build

      config.tunnelConfig.fold(settings.pure[Resource[F, ?]])(viaTunnel(_, settings, blockingPool))
    })

  def connectionString[F[_]: Sync](s: String): F[ConnectionString] = Sync[F].delay {
    new ConnectionString(s)
  }

  def mkJSch[F[_]: Sync]: F[JSch] = Sync[F].delay { new JSch() }

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def mkSession[F[_]: Sync](jsch: JSch, cfg: TunnelConfig): F[Session] = cfg.pass match {
    case None =>
      Sync[F].delay { jsch.getSession(cfg.user, cfg.host, cfg.port) }
    case Some(cred) => cred match {
      case Password(password) => for {
        s <- Sync[F].delay { jsch.getSession(cfg.user, cfg.host, cfg.port) }
        _ <- Sync[F].delay { s.setPassword(password) }
      } yield s
      case Identity(prv, passphrase) => for {
        _ <- Sync[F].delay { jsch.addIdentity(SessionName, prv.getBytes("UTF-8"), null, passphrase map (_.getBytes("UTF-8")) getOrElse null) }
        s <- Sync[F].delay { jsch.getSession(cfg.user, cfg.host, cfg.port) }
      } yield s
    }
  }

  def userInfo(cfg: TunnelConfig): UserInfo = new UserInfo {
    override def getPassword(): String = cfg.pass match {
      case None => ""
      case Some(pass) => pass match {
        case Identity(_, _) => ""
        case Password(p) => p
      }
    }
    override def getPassphrase(): String = cfg.pass match {
      case None => ""
      case Some(pass) => pass match {
        case Identity(_, p) => p getOrElse ""
        case Password(_) => ""
      }
    }
    override def promptYesNo(s: String): Boolean = true
    override def promptPassphrase(s: String): Boolean = true
    override def promptPassword(s: String): Boolean = true
    override def showMessage(s: String): Unit = ()
  }

  def setUserInfo[F[_]: Sync](s: Session, u: UserInfo): F[Unit] = Sync[F].delay {
    s.setUserInfo(u)
  }

  def serverAddress[F[_]: Sync](settings: ClusterSettings): F[ServerAddress] = {
    val hosts = settings.getHosts
    if (hosts.size < 1) Sync[F].raiseError(new Throwable("Server addresses were not provided"))
    else Sync[F].delay { hosts.get(0) }
  }

  final case class Tunnel(port: Int)

  def openTunnel[F[_]: Sync](session: Session, address: ServerAddress): F[Tunnel] = Sync[F].delay {
    session.connect()
  } productR Sync[F].delay {
    session.setPortForwardingL(0, address.getHost, address.getPort)
  } map (Tunnel(_))

  def closeTunnel[F[_]: Sync](session: Session, tunnel: Tunnel): F[Unit] = Sync[F].delay {
    session.delPortForwardingL(tunnel.port)
  } productR Sync[F].delay {
    session.disconnect()
  }

  def tunneledSettings[F[_]: Sync](settings: ClusterSettings, tunnel: Tunnel): F[ClusterSettings] = Sync[F].delay {
    ClusterSettings.builder(settings).hosts(List(new ServerAddress("localhost", tunnel.port)).asJava).build
  }

  def viaTunnel[F[_]: Sync: ContextShift](
      config: TunnelConfig,
      settings: ClusterSettings,
      blockingPool: BlockingContext)
      : Resource[F, ClusterSettings] =
    Resource(ContextShift[F].evalOn(blockingPool.unwrap)(for {
      jsch <- mkJSch
      sess <- mkSession(jsch, config)
      _ <- setUserInfo(sess, userInfo(config))
      address <- serverAddress(settings)
      tunnel <- openTunnel(sess, address)
      result <- tunneledSettings(settings, tunnel)
    } yield (result, ContextShift[F].evalOn(blockingPool.unwrap)(closeTunnel(sess, tunnel)))))
}
