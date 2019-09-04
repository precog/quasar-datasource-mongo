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
import java.security.{SecureRandom, KeyStore, KeyFactory}
import java.security.cert.{X509Certificate, CertificateFactory}
import javax.net.ssl.{SSLContext, KeyManager, TrustManager, X509TrustManager, KeyManagerFactory, TrustManagerFactory}
import java.security.spec.{X509EncodedKeySpec, PKCS8EncodedKeySpec, RSAPrivateCrtKeySpec, KeySpec}

import sun.security.util.{DerInputStream, DerValue}

object SSL {
  def context[F[_]: Sync](config: SSLConfig): F[SSLContext] = for {
    ctx <- Sync[F].delay(SSLContext.getInstance("TLS"))
    ks <- keyManagers[F](config)
    ts <- trustManagers[F](config)
    rnd <- Sync[F].delay(new SecureRandom())
    _ <- Sync[F].delay(ctx.init(ks, ts, rnd))
  } yield ctx

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def keyManagers[F[_]: Sync](config: SSLConfig): F[Array[KeyManager]] = config.clientPEM match {
    case None => Sync[F].delay(null)
    case Some(prv) =>
      val beginCertificate: String = "-----BEGIN CERTIFICATE-----"
      val endCertificate: String = "-----END CERTIFICATE-----"
      val beginRSAKey: String = "-----BEGIN RSA PRIVATE KEY-----"
      val endRSAKey: String = "-----END RSA PRIVATE KEY-----"
      val beginPKCS8: String = "-----BEGIN PRIVATE KEY-----"
      val endPKCS8: String = "-----END PRIVATE KEY-----"

      def between(start: String, end: String, hay: String): Option[String] = {
        val startIx: Int = hay.indexOfSlice(start)
        val endIx: Int = hay.indexOfSlice(end)
        if (startIx < 0 || endIx < 0) None
        else Some(prv.substring(startIx, endIx + end.length))

      }

      val certificate: String = between(beginCertificate, endCertificate, prv).get

      def getKey(start: String, end: String, hay: String): Option[Array[Byte]] = between(start, end, hay).map { (x: String) =>
        java.util.Base64.getDecoder.decode(x.replace("\r", "").replace("\n", "").replace(start, "").replace(end, ""))
      }

      val rsaKey = getKey(beginRSAKey, endRSAKey, prv)

      val pkcs8Key = getKey(beginPKCS8, endPKCS8, prv)

      val keySpec: F[Option[KeySpec]] = rsaKey match {
        case Some(bytes) => for {
          derReader <- Sync[F].delay(new DerInputStream(bytes))
          (ders: Array[DerValue]) <- Sync[F].delay(derReader.getSequence(0))
          keySpec <- Sync[F].delay {
            new RSAPrivateCrtKeySpec(
              ders(1).getBigInteger,
              ders(2).getBigInteger,
              ders(3).getBigInteger,
              ders(4).getBigInteger,
              ders(5).getBigInteger,
              ders(6).getBigInteger,
              ders(7).getBigInteger,
              ders(8).getBigInteger)
          }
        } yield Some(keySpec)
        case None => pkcs8Key match {
          case None => Sync[F].delay(None)
          case Some(bytes) => Sync[F].delay(Some(new PKCS8EncodedKeySpec(bytes)))
        }
      }

      for {
        spec <- keySpec
        result <- spec match {
          case None => Sync[F].delay(null)
          case Some(s) => for {
            certificate <- Sync[F].delay {
              CertificateFactory.getInstance("X509").generateCertificate(new ByteArrayInputStream(certificate.getBytes("UTF-8")))
            }
            ks <- Sync[F].delay(KeyStore.getInstance(KeyStore.getDefaultType()))
            pwd = "password".toCharArray
            _ <- Sync[F].delay(ks.load(null, pwd))
            keyFactory <- Sync[F].delay {
              KeyFactory.getInstance("RSA")
            }
            key <- Sync[F].delay {
              keyFactory.generatePrivate(s)
            }
            _ <- Sync[F].delay {
              ks.setKeyEntry("default", key, pwd, Array(certificate))
            }
            km <- Sync[F].delay(KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm()))
            _ <- Sync[F].delay(km.init(ks, pwd))
            res <- Sync[F].delay(km.getKeyManagers())
          } yield res
        }
      } yield result
  }

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def trustManagers[F[_]: Sync](config: SSLConfig): F[Array[TrustManager]] = config.serverCA match {
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
