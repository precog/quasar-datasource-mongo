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

import cats.data.OptionT
import cats.effect.Sync
import cats.syntax.functor._
import cats.syntax.flatMap._
import cats.syntax.traverse._
import cats.instances.option._

import java.io.ByteArrayInputStream
import java.security.{SecureRandom, KeyStore, KeyFactory}
import java.security.cert.{X509Certificate, CertificateFactory}
import java.security.spec.{PKCS8EncodedKeySpec, RSAPrivateCrtKeySpec, KeySpec}
import javax.crypto.{Cipher, EncryptedPrivateKeyInfo, SecretKeyFactory}
import javax.crypto.spec.PBEKeySpec
import javax.net.ssl.{SSLContext, KeyManager, TrustManager, X509TrustManager, KeyManagerFactory, TrustManagerFactory}

import sun.security.util.DerInputStream

object SSL {
  def context[F[_]: Sync](config: SSLConfig): OptionT[F, SSLContext] = {
    val F = Sync[OptionT[F, ?]]
    for {
      ctx <- F.delay(SSLContext.getInstance("TLS"))
      ks <- keyManagers[F](config)
      ts <- trustManagers[F](config)
      rnd <- F.delay(new SecureRandom())
      _ <- F.delay(ctx.init(ks, ts, rnd))
    } yield ctx
  }

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def keyManagers[F[_]: Sync](config: SSLConfig): OptionT[F, Array[KeyManager]] = {
    val F = Sync[OptionT[F, ?]]
    config.clientPEM match {
      case None => F.delay(null)
      case Some(prv) => for {
        certificate <- OptionT.fromOption[F] {
          val begin: String = "-----BEGIN CERTIFICATE-----"
          val end: String = "-----END CERTIFICATE-----"
          between(begin, end, prv)
        }
        pkey <- OptionT(PrivateKey.mkKey(prv))
        spec <- PrivateKey.keySpec[OptionT[F, ?]](pkey, config.passphrase.getOrElse(""))
        certificate <- F.delay {
          CertificateFactory.getInstance("X509").generateCertificate(new ByteArrayInputStream(certificate.getBytes("UTF-8")))
        }
        ks <- F.delay(KeyStore.getInstance(KeyStore.getDefaultType()))
        pwd = "password".toCharArray
        _ <- F.delay(ks.load(null, pwd))
        keyFactory <- F.delay(KeyFactory.getInstance("RSA"))
        key <- F.delay(keyFactory.generatePrivate(spec))
        _ <- F.delay(ks.setKeyEntry("default", key, pwd, Array(certificate)))
        km <- F.delay(KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm()))
        _ <- F.delay(km.init(ks, pwd))
        res <- F.delay(km.getKeyManagers())
      } yield res
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def trustManagers[F[_]: Sync](config: SSLConfig): OptionT[F, Array[TrustManager]] = {
    val F = Sync[OptionT[F, ?]]
    config.serverCA match {
      case None =>
        val fTM: OptionT[F, TrustManager] = F.delay { new X509TrustManager {
          def getAcceptedIssuers(): Array[X509Certificate] = Array()
          def checkClientTrusted(certs: Array[X509Certificate], authType: String): Unit = ()
          def checkServerTrusted(certs: Array[X509Certificate], authType: String): Unit = ()
        }}
        fTM.map(Array(_))
      case Some(ca) =>
        for {
          ks <- F.delay(KeyStore.getInstance(KeyStore.getDefaultType()))
          pwd = "password".toCharArray
          _ <- F.delay(ks.load(null, pwd))
          cf <- F.delay {
            CertificateFactory.getInstance("X509").generateCertificate(new ByteArrayInputStream(ca.getBytes("UTF-8")))
          }
          _ <- F.delay(ks.setCertificateEntry("default", cf))
          ts <- F.delay(TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm()))
          _ <- F.delay(ts.init(ks))
          res <- F.delay(ts.getTrustManagers())
        } yield res
    }
  }


  private def between(start: String, end: String, hay: String): Option[String] = {
    val startIx: Int = hay.indexOfSlice(start)
    val endIx: Int = hay.indexOfSlice(end)
    if (startIx < 0 || endIx < 0) None
    else Some(hay.substring(startIx, endIx + end.length))
  }

  trait PrivateKey extends Product with Serializable
  object PrivateKey {
    final case class PKCS1(bytes: Vector[Byte]) extends PrivateKey
    final case class PKCS8(bytes: Vector[Byte]) extends PrivateKey
    final case class EncryptedPKCS8(bytes: Vector[Byte]) extends PrivateKey

    val PKCS1Tag: String = "RSA PRIVATE KEY"
    val PKCS8Tag: String = "PRIVATE KEY"
    val EncryptedPKCS8Tag: String = "ENCRYPTED PRIVATE KEY"

    private def taggedBytes[F[_]: Sync](tag: String, hay: String): F[Option[Array[Byte]]] = {
      val start = s"-----BEGIN $tag-----"
      val end = s"-----END $tag-----"
      between(start, end, hay).traverse { (x: String) => Sync[F].delay {
        java.util.Base64.getDecoder.decode(x.replace("\r", "").replace("\n", "").replace(start, "").replace(end, ""))
      }}
    }
    def mkKey[F[_]: Sync](inp: String): F[Option[PrivateKey]] = for {
      pkcs1Bytes <- taggedBytes(PKCS1Tag, inp)
      pkcs8Bytes <- taggedBytes(PKCS8Tag, inp)
      encBytes <- taggedBytes(EncryptedPKCS8Tag, inp)
    } yield {
      pkcs1Bytes.map(x => PKCS1(x.toVector)) orElse
      pkcs8Bytes.map(x => PKCS8(x.toVector)) orElse
      encBytes.map(x => EncryptedPKCS8(x.toVector))
    }

    def keySpec[F[_]: Sync](key: PrivateKey, pwd: String): F[KeySpec] = {
      val F = Sync[F]
      key match {
        case PKCS1(bytes) => for {
          reader <- F.delay(new DerInputStream(bytes.toArray))
          ders <- F.delay(reader.getSequence(0))
        } yield {
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
        case PKCS8(bytes) =>
          F.delay(new PKCS8EncodedKeySpec(bytes.toArray))
        case EncryptedPKCS8(bytes) => for {
          info <- F.delay(new EncryptedPrivateKeyInfo(bytes.toArray))
          cipher <- F.delay(Cipher.getInstance(info.getAlgName()))
          pbeKeySpec <- F.delay(new PBEKeySpec(pwd.toCharArray))
          secretFactory <- F.delay(SecretKeyFactory.getInstance(info.getAlgName()))
          pbeKey <- F.delay(secretFactory.generateSecret(pbeKeySpec))
          algParams <- F.delay(info.getAlgParameters())
          _ <- F.delay(cipher.init(Cipher.DECRYPT_MODE, pbeKey, algParams))
          result <- F.delay(info.getKeySpec(cipher))
        } yield result
      }
    }
  }
}
