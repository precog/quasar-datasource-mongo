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

import cats.effect.Sync
import cats.syntax.functor._
import cats.syntax.flatMap._

import java.io.ByteArrayInputStream
import java.security.{SecureRandom, KeyStore}
import java.security.cert.{X509Certificate, CertificateFactory}
import javax.net.ssl.{SSLContext, KeyManager, TrustManager, X509TrustManager, KeyManagerFactory, TrustManagerFactory}

object SSL {
  def context[F[_]: Sync](config: SSLConfig): F[SSLContext] = for {
    ctx <- Sync[F].delay(SSLContext.getInstance("TLS"))
    ks <- keyManagers[F](config)
    ts <- trustManagers[F](config)
    rnd <- Sync[F].delay(new SecureRandom())
    _ <- Sync[F].delay(ctx.init(ks, ts, rnd))
  } yield ctx

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def keyManagers[F[_]: Sync](config: SSLConfig): F[Array[KeyManager]] = config.clientPEM match {
    case None => Sync[F].delay(null)
    case Some(prv) => for {
      ks <- Sync[F].delay(KeyStore.getInstance(KeyStore.getDefaultType()))
      pwd = "password".toCharArray
      _ <- Sync[F].delay(ks.load(null, pwd))
      km <- Sync[F].delay(KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm()))
      _ <- Sync[F].delay(km.init(ks, pwd))
      res <- Sync[F].delay(km.getKeyManagers())
    } yield res
  }

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def trustManagers[F[_]: Sync](config: SSLConfig): F[Array[TrustManager]] = config.serverCA match {
    case None =>
      val fTM: F[TrustManager] = Sync[F].delay { new X509TrustManager {
        def getAcceptedIssuers(): Array[X509Certificate] = Array()
        def checkClientTrusted(certs: Array[X509Certificate], authType: String): Unit = ()
        def checkServerTrusted(certs: Array[X509Certificate], authType: String): Unit = ()
      }}
      fTM.map(Array(_))
    case Some(ca) => for {
      ks <- Sync[F].delay(KeyStore.getInstance(KeyStore.getDefaultType()))
      pwd = "password".toCharArray
      _ <- Sync[F].delay(ks.load(null, pwd))
      cf <- Sync[F].delay {
        CertificateFactory.getInstance("X509").generateCertificate(new ByteArrayInputStream(ca.getBytes("UTF-8")))
      }
      _ <- Sync[F].delay(ks.setCertificateEntry("default", cf))
      ts <- Sync[F].delay(TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm()))
      _ <- Sync[F].delay(ts.init(ks))
      res <- Sync[F].delay(ts.getTrustManagers())
    } yield res
  }
}
