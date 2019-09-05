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

import cats.effect.{IO, Resource}
import cats.instances.list._
import cats.syntax.apply._
import cats.syntax.traverse._

import quasar.concurrent.BlockingContext
import quasar.connector.ResourceError
import quasar.physical.mongo.MongoResource.{Collection, Database}
import quasar.EffectfulQSpec

import org.bson.{Document => _, _}
import org.mongodb.scala.{Completed, Document, MongoSecurityException, MongoTimeoutException}
import org.specs2.specification.core._
import org.specs2.execute.AsResult
import scala.io.Source

import shims._
import testImplicits._

class MongoSpec extends EffectfulQSpec[IO] {
  import MongoSpec._
  sequential
/*  step(MongoSpec.setupDB.unsafeRunSync())

  "can create client from valid connection string" >>* mkMongo.use(IO.pure).attempt.map(_ must beRight)

  "can't create client from incorrect connection string" >> {
    "for incorrect protocol" >> {
      Mongo[IO](
        MongoConfig.basic("http://localhost"),
        BlockingContext.cached("not-used"))
        .use(IO.pure)
        .unsafeRunSync() must throwA[java.lang.IllegalArgumentException]
    }

    "for unreachable config" >> {
      Mongo[IO](
        MongoConfig.basic("mongodb://unreachable"),
        BlockingContext.cached("not-used"))
        .use(IO.pure)
        .unsafeRunSync() must throwA[MongoTimeoutException]
    }
  }

  "getting databases works correctly" >>* mkMongo.use { mongo =>
    mongo.databases.compile.toList.map { evaluatedDbs =>
      MongoSpec.correctDbs.toSet.subsetOf(evaluatedDbs.toSet)
    }
  }

  "getting databases for constrained role works correctly A" >>* mkAMongo.use { mongo =>
    mongo.databases.compile.toList.map { _ === List(Database("A")) }
  }

  "getting databases for constrained role works correctly B" >>* mkBMongo.use { mongo =>
    mongo.databases.compile.toList.map { _ === List(Database("B")) }
  }

  "it's impossible to make mongo with incorrect auth" >> {
    mkInvalidAMongo.use(IO.pure).unsafeRunSync() must throwA[MongoSecurityException]
  }

  "databaseExists returns true for existing dbs" >> Fragment.foreach(MongoSpec.correctDbs)(db =>
      s"checking ${db.name}" >>* mkMongo.use { mongo =>
        mongo.databaseExists(db).compile.lastOrError
      }
  )

  "databaseExists returns false for non-existing dbs" >> Fragment.foreach(MongoSpec.incorrectDbs)(db =>
    s"checking ${db.name}" >>* mkMongo.use { mongo =>
      mongo.databaseExists(db).map(!_).compile.lastOrError
    }
  )

  "collections returns correct collection lists" >> Fragment.foreach(MongoSpec.correctDbs)(db =>
    s"checking ${db.name}" >>* mkMongo.use { mongo =>
      mongo.collections(db)
        .fold(List[Collection]())((lst, coll) => coll :: lst)
        .map(collectionList => collectionList.toSet === MongoSpec.cols.map(c => Collection(db, c)).toSet)
        .compile
        .lastOrError
    }
  )

  "collections returns correct collection lists for constrained roles B" >> {
    "B" >>* mkBMongo.use { mongo =>
      mongo.collections(Database("B")).compile.toList.map { _ === List() }
    }

    "B.b" >>* mkBBMongo.use { mongo =>
      mongo.collections(Database("B")).compile.toList.map { _ == List(Collection(Database("B"), "b")) }
    }
  }

  "collections return empty stream for non-existing databases" >> Fragment.foreach(MongoSpec.incorrectDbs)(db =>
    s"checking ${db.name}" >>* mkMongo.use { mongo =>
      mongo.collections(db).compile.toList.map(_ === List[Collection]())
    }
  )

  "collectionExists returns true for existent collections" >> Fragment.foreach(MongoSpec.correctCollections)(col =>
    s"checking ${col.database.name} :: ${col.name}" >>* mkMongo.use { mongo =>
      mongo.collectionExists(col).compile.lastOrError
    }
  )

  "collectionExists returns false for non-existent collections" >> Fragment.foreach(MongoSpec.incorrectCollections)(col =>
    s"checking ${col.database.name} :: ${col.name}" >>* mkMongo.use { mongo =>
      mongo.collectionExists(col).map(!_).compile.lastOrError
    }
  )

  "findAll returns correct results for existing collections" >> Fragment.foreach(MongoSpec.correctCollections)(col =>
    s"checking ${col.database.name} :: ${col.name}" >>* mkMongo.use { mongo =>
      mongo.findAll(col).flatMap { stream =>
        stream
          .fold(List[BsonValue]())((lst, col) => col :: lst)
          .map(bsons => bsons match {
            case (bson: BsonDocument) :: List() => AsResult(bson.getString(col.name).getValue() === col.database.name)
            case _ => AsResult(false)
          })
          .compile
          .lastOrError
      }
    }
  )

  "evaluteImpl works falls back if there is an exception" >> Fragment.foreach(MongoSpec.correctCollections)(col =>
    s"checking ${col.database.name} :: ${col.name}" >>* mkMongo.use { mongo =>
      mongo.evaluateImpl(col, incorrectPipeline, mongo.findAll(col)).flatMap { case (_, stream) =>
        stream
          .fold(List[BsonValue]())((lst, col) => col :: lst)
          .map(bsons => bsons match {
            case (bson: BsonDocument) :: List() => AsResult(bson.getString(col.name).getValue() === col.database.name)
            case _ => AsResult(false)
          })
          .compile
          .lastOrError
      }
    }
  )

  "findAll raises path not found for nonexistent collections" >> Fragment.foreach(MongoSpec.incorrectCollections)(col =>
    s"checking ${col.database.name} :: ${col.name}" >>* mkMongo.use { mongo =>
      mongo.findAll(col).attempt.map(_ must beLike {
        case Left(t) => ResourceError.throwableP.getOption(t) must_=== Some(ResourceError.pathNotFound(col.resourcePath))
      })
    }
  )
 */
  "ssl" >> {
/*
    "can't connect w/o any ssl configuration" >>* {
      Mongo[IO](MongoConfig.basic(insecuredConnectionString), blockingPool)
        .use(IO.pure)
        .attempt
        .map(_ must beLeft)
    }

    "can connect when ssl enabled" >>* {
      val sslConfig = SSLConfig(None, None, true, None)
      Mongo[IO](MongoConfig.basic(securedConnectionString).useSSL(sslConfig), blockingPool)
        .use(IO.pure)
        .attempt
        .map(_ must beRight)
    }

    "can connect when ssl enabled via connectionString only" >>* {
      Mongo[IO](MongoConfig.basic(securedConnectionString), blockingPool)
        .use(IO.pure)
        .attempt
        .map(_ must beRight)
    }

    "can connect with certificate authority provided" >>* {
      val certString = """
-----BEGIN CERTIFICATE-----
MIIGFTCCA/2gAwIBAgIJAJjzUnLXxIlrMA0GCSqGSIb3DQEBCwUAMIGgMQswCQYD
VQQGEwJVUzELMAkGA1UECAwCQ08xITAfBgNVBAoMGEludGVybmV0IFdpZGdpdHMg
UHR5IEx0ZDExMC8GA1UECwwoVHJhdmlzIHRlc3RzIGZvciBxdWFzYXItZGF0YXNv
dXJjZS1tb25nbzENMAsGA1UEAwwEcm9vdDEfMB0GCSqGSIb3DQEJARYQYm90QHNs
YW1kYXRhLmNvbTAeFw0xOTA5MDIxMjMyMTZaFw0yMjA2MjIxMjMyMTZaMIGgMQsw
CQYDVQQGEwJVUzELMAkGA1UECAwCQ08xITAfBgNVBAoMGEludGVybmV0IFdpZGdp
dHMgUHR5IEx0ZDExMC8GA1UECwwoVHJhdmlzIHRlc3RzIGZvciBxdWFzYXItZGF0
YXNvdXJjZS1tb25nbzENMAsGA1UEAwwEcm9vdDEfMB0GCSqGSIb3DQEJARYQYm90
QHNsYW1kYXRhLmNvbTCCAiIwDQYJKoZIhvcNAQEBBQADggIPADCCAgoCggIBAMkN
PLjWunQbKHJ93W+gT/UdpkYqp/N1bv8uxZiYhUjE+xm2dfRa8IJJRuxxx6RjJVhL
9WL4/pruGY0lrQPj4K/rqOWtGyzkLMQAvUBft0hgtVg9h/Eig7IiPQpyhBPQfu73
gSCIfviZX0jlcbC/HhhIO/wG185lMXG2Jpvztu6uFWISdoQn/WKnkzv6w9TcUTjS
ZEhiklXlRIj4S/65x7l0PJdjZM86BF1cWipHwcHwmkHJxVReZky0o5psRApLUBzy
aNKcmGBbLtdgdTQKTZhNhfYH1GxCqNopUeBE84/XvKx/8WFFf6LQ6e2lN1+qzM7U
0zg0iqYrYgyiHWWQJaIY0rksmfQWyaNimvNKRA1HfEzHW9aFmgkt2K4ncQ8BwNm7
dFPiToKQWCyK2vvYof/QUcb/f9JFlhcFaoHxdjvbV52pxnEEqBo8l9J94opRcx94
PuTByYYs+MLLO1vuqeilVAp+8bUfvQtsR/4Ri8vgd/Egy18nUPCDyLePj4WnuIKH
QEGQwzVR6nmkwQlPrDMPa0HGvQk5HevePcTErP98SjE6MkyzACO+a96A5w9QF8X/
TiNTXI28ieTbRvog9q+oSW1GBEEDVOMrBm+WubA5NpNLc9HGrBYFhi4XYzXRM7RX
BLeqChZrcOP/79Ym9J22MfHLmZH1hdkWKY1ralIRAgMBAAGjUDBOMB0GA1UdDgQW
BBSLuYAnqAU5BRzxjU5EDzymFejknjAfBgNVHSMEGDAWgBSLuYAnqAU5BRzxjU5E
DzymFejknjAMBgNVHRMEBTADAQH/MA0GCSqGSIb3DQEBCwUAA4ICAQBvGo6DgfNc
/dQJ0ROZVtW4TPa0Pa+t8SydtPS3533wdZmqlDZWNhRPvENVKyowMhyDCmmdcTpn
X9FOdVMHf9WMeH1BDedjrqHpcLWHnOk/RMh6kJ95Qg15rdMTFUmLPv4XbWhh1Sof
SNZB2QXMG9G9Lu8Vg1DRrKs8TCcuaCZfnKERA4CiL0G6DTLd1os6QxZcOdrI+0PR
+9zErCvxTgRjO8cVLvLnkjBScrsEVXRX7KhHJQAv/jHlZHCW44IpXicEHGHGZn0F
QAHMb92NEaF10+GbZAoXi0Q2emDP+R4YDBNjG+wk2gcHLAnxDjbrhtPRrjUkPpiV
GdiNNFkplIJnCZepHiGc1Lhc477lP+MRFjfBdjAksPn4ECd902JdjUI4wFqnIU/y
mRp+TykVv9NmFWkhbf+T1gzt4S1eaWZT5BLp4x3jimOQvkEFXYLkfVwWRT/e3oJp
TtZYRyUgo/772y6/WwUoC7JVtDrAP4c78EyjY3tB2D5F4ruDQWN2s48ziautg7cM
KogCnn7keHB3k1PxmM+y8L4FLB8QG+54z9uMO9Q2bSIjB1PPHmY3GKLqOljmN3pz
Il6xnf3gHCMdRqP+++aZhIwtWGECD57n5CoP25iPelSVp9CmBKuu8HGFTEajofFK
B8nj9xIMi13bXa9zlWQquzo5WofYVSJxjw==
-----END CERTIFICATE-----
"""
      val sslConfig = SSLConfig(Some(certString), None, true, None)
      Mongo[IO](MongoConfig.basic(securedConnectionString).useSSL(sslConfig), blockingPool)
        .use(IO.pure)
        .attempt
        .map(_ must beRight)
    }
    "can't connect with incorrect certificate" >>* {
      val sslConfig = SSLConfig(Some("incorrect certificate"), None, true, None)
      Mongo[IO](MongoConfig.basic(securedConnectionString).useSSL(sslConfig), blockingPool)
        .use(IO.pure)
        .attempt
        .map(_ must beLeft)
    }
 */
    "pkcs1" >>* {
      val privateKey = """
-----BEGIN RSA PRIVATE KEY-----
MIIEogIBAAKCAQEA1SF4dWUdM/vNrJ0R7OlnfygimlwCVASWZD+Esom0UPdHuKdXPLlPATFcKJ+ChYk7MnHhNpb4gqLtMoiHqahqTj6z4MxSvb6+XI8BM/3dyetHf4LYrJY1UNpj//0HsjLPh0mFlWCMxbMI0ojPIga7377WqngPpqMeIGxMcx7KgF8Z9RspGDzf7vxMFM/uY7PB5lYID8Y2xro2thO+XSVyV2wmLwoFmajPomtLtVjUP6I7qWwImcn6fBG/PmVrArqrqxae8WRHioEPjHhznhsAc5CKrWYMduuMienaj0Giqu3vuq3YVV+5R+SN6Jg3TRaZ5j3nUoA9xSGR7AZ/RVarMQIDAQABAoIBADorc3QfD9RjUmRdTkop/gxviNo2aJBD3EQqLe6ItQUwKIwWaDHzpcN+lFemCXqm1NhUypIarGMDUtfq+ZPtmm4WoQOm4KJXEB+1DDADhugYZiBANyELsiuKH6b7iYEyav/Sqjn16xX+YlcL7fSsf9R2gqJuOF9Tv+I7jLYS3lQgoRPWF0k0bwQK1MuTmTV0towe0WTWmkAAhU71IaHaYFtRPFsyQQkctsz20Uj0/Fv6Oluy72aL3u24ry80FE61u1MeCP0/+39sMNwrOwX/rx2BIliOgb+mOCWq2fBn0gX4fxbXkrL8FER8g+EX2XwWbnWJl82FP+JuR4GqwfA2Jx0CgYEA9EWc98kCOpmZ1Sku79JuB524gZ2cahXH2aYdXCKjkVUXq7sQIPWDJrR3D+D3AMpzs472CKJqN6P7Khps1YkZ5B0YAz8NqFJvx6if6qRfCEQFAcON2BpuVW0dVCi5FhpsXV/4gYjU6GpHl3o/t0m1j8/B++vy08pvmY9nYE6/ve8CgYEA310Yj5QwS4hlIP/NowbVNMTLwpcWphU4aqqRxpXCz084Y27bpf31fJhm5gDcNDjgaVyxaGityKHLM8bwVTPe1g4RcPw/Dbd6Xsxqak0thRIv+468a6HcNRibAsh1w/gwjNqIxyBfCBooA3JBvE+T5Yyvy8FGnuVEhtMt0WRrSN8CgYAzs1QwwbOeEYqUqj3L9p7hL8mwbVqAvZFqCJWoPZCfHwJ+j34va6dRltqoWrYMzczFUVnTpASVODjdxXAJlhYLyPifH0ZVvPT5rkACmr2ecz8YC3rHJXj6zbhqaqAaBIexD7H6QKrHck8qyW4Y7hnmkVN+bYccunj6aHa51JESswKBgHLC2kOq18V3Jr51r6n7cL/T/PzVtAVREdN8H7nwaE8rXMV5x9DZUq/ZfcTe3ETYlDSOM3h5kBBtUIIhzIl6RRulzsBGWf8qLe1XCbXkQWcFmTGefKCwYPvG0J31cMxnUEqOXYgx0aHQDo72vV+LY2mlhw0hGK+7DCJZjkjvV0/xAoGABTA5OhewWFhLgeVXiL7pGCrHwe1nL47CVRtbQdpc5wKA7rVSK385xMI22w3GuB/kGAThldiX0stviQa1iGvW0lOOnJV27ZAf6OFZWdZK/P96he8l0rgxXBhoxb669Wz1YxQN5BBQLHcqM8ltApU8kL9HNT9q3DF1KWQsd1ALkgY=
-----END RSA PRIVATE KEY-----
-----BEGIN CERTIFICATE-----
MIIEpDCCAowCCQCcDhAKaNop2zANBgkqhkiG9w0BAQsFADCBoDELMAkGA1UEBhMC
VVMxCzAJBgNVBAgMAkNPMSEwHwYDVQQKDBhJbnRlcm5ldCBXaWRnaXRzIFB0eSBM
dGQxMTAvBgNVBAsMKFRyYXZpcyB0ZXN0cyBmb3IgcXVhc2FyLWRhdGFzb3VyY2Ut
bW9uZ28xDTALBgNVBAMMBHJvb3QxHzAdBgkqhkiG9w0BCQEWEGJvdEBzbGFtZGF0
YS5jb20wHhcNMTkwOTAyMTIzNTAwWhcNMjIwNjIyMTIzNTAwWjCBhjELMAkGA1UE
BhMCVVMxCzAJBgNVBAgMAkNPMSEwHwYDVQQKDBhJbnRlcm5ldCBXaWRnaXRzIFB0
eSBMdGQxMTAvBgNVBAsMKFRyYXZpcyB0ZXN0cyBmb3IgcXVhc2FyLWRhdGFzb3Vy
Y2UtbW9uZ28xFDASBgNVBAMMC3Rlc3Rpbmcga2V5MIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEA1SF4dWUdM/vNrJ0R7OlnfygimlwCVASWZD+Esom0UPdH
uKdXPLlPATFcKJ+ChYk7MnHhNpb4gqLtMoiHqahqTj6z4MxSvb6+XI8BM/3dyetH
f4LYrJY1UNpj//0HsjLPh0mFlWCMxbMI0ojPIga7377WqngPpqMeIGxMcx7KgF8Z
9RspGDzf7vxMFM/uY7PB5lYID8Y2xro2thO+XSVyV2wmLwoFmajPomtLtVjUP6I7
qWwImcn6fBG/PmVrArqrqxae8WRHioEPjHhznhsAc5CKrWYMduuMienaj0Giqu3v
uq3YVV+5R+SN6Jg3TRaZ5j3nUoA9xSGR7AZ/RVarMQIDAQABMA0GCSqGSIb3DQEB
CwUAA4ICAQA4CX0gwXBFeQDOOGfaU0MFlSFl35ztnszybOsX7dIG9Cv0+j9MEXoP
yjLLSFMmMGKTWLOw69kIhrOfMC5tbgrHjYjiSc7WT39mCpxcNYOSvn4A+Fcz488w
I07Hr7etq29R38ICnCnpwx7GzW89e1s4IafL9GMh8hWWMI9Ig3dUPdxxIJpGQPtG
dv6m9mGkEQcvEXdLaLHEsEVMgSru1rkEJ8ciOrF8+v4+b/IzW1Lz18BgTjXSm4b5
8RQdnnhxz5MWr6urDTOgzR549P+j02yvcJrqiFJC5CbUedRWrdYG3atd6OhQQeYQ
BnebYiPxKpsOq4rNBz5RHHOMybitfk3hnhCHGb3rtkkRZ9yZkCalw4qU/30Kbkjl
zvpZU7HmnJ20mcDrerJUEMZN64sndQByHo22rIH1+b3F41VTyf/vpEnhxP0B9gLd
4bQJ90lQw0gooTUOvIJIhyPBq1+5E7CWLZsRT6gXqx2W1iobw+kVoAqxPlZ7wLHM
OSVPuFJTC88/nqHC08SG6dZ8jWHY0eWW3a1G8FhwvmIZ6MnskFQ9To+ylVObT2zB
+voA2aAAr9mnnC219udk236lqa7aaTWBhR79u2xU39dKicToiwta+jHbR8H8k/d5
RZR1+hOFgcehjPs+6wehxmXaWUJBZFbLdE6uQJljxxsmpHL/YfQ3Zw==
-----END CERTIFICATE-----

"""
      val certString = """
-----BEGIN CERTIFICATE-----
MIIGFTCCA/2gAwIBAgIJAJjzUnLXxIlrMA0GCSqGSIb3DQEBCwUAMIGgMQswCQYD
VQQGEwJVUzELMAkGA1UECAwCQ08xITAfBgNVBAoMGEludGVybmV0IFdpZGdpdHMg
UHR5IEx0ZDExMC8GA1UECwwoVHJhdmlzIHRlc3RzIGZvciBxdWFzYXItZGF0YXNv
dXJjZS1tb25nbzENMAsGA1UEAwwEcm9vdDEfMB0GCSqGSIb3DQEJARYQYm90QHNs
YW1kYXRhLmNvbTAeFw0xOTA5MDIxMjMyMTZaFw0yMjA2MjIxMjMyMTZaMIGgMQsw
CQYDVQQGEwJVUzELMAkGA1UECAwCQ08xITAfBgNVBAoMGEludGVybmV0IFdpZGdp
dHMgUHR5IEx0ZDExMC8GA1UECwwoVHJhdmlzIHRlc3RzIGZvciBxdWFzYXItZGF0
YXNvdXJjZS1tb25nbzENMAsGA1UEAwwEcm9vdDEfMB0GCSqGSIb3DQEJARYQYm90
QHNsYW1kYXRhLmNvbTCCAiIwDQYJKoZIhvcNAQEBBQADggIPADCCAgoCggIBAMkN
PLjWunQbKHJ93W+gT/UdpkYqp/N1bv8uxZiYhUjE+xm2dfRa8IJJRuxxx6RjJVhL
9WL4/pruGY0lrQPj4K/rqOWtGyzkLMQAvUBft0hgtVg9h/Eig7IiPQpyhBPQfu73
gSCIfviZX0jlcbC/HhhIO/wG185lMXG2Jpvztu6uFWISdoQn/WKnkzv6w9TcUTjS
ZEhiklXlRIj4S/65x7l0PJdjZM86BF1cWipHwcHwmkHJxVReZky0o5psRApLUBzy
aNKcmGBbLtdgdTQKTZhNhfYH1GxCqNopUeBE84/XvKx/8WFFf6LQ6e2lN1+qzM7U
0zg0iqYrYgyiHWWQJaIY0rksmfQWyaNimvNKRA1HfEzHW9aFmgkt2K4ncQ8BwNm7
dFPiToKQWCyK2vvYof/QUcb/f9JFlhcFaoHxdjvbV52pxnEEqBo8l9J94opRcx94
PuTByYYs+MLLO1vuqeilVAp+8bUfvQtsR/4Ri8vgd/Egy18nUPCDyLePj4WnuIKH
QEGQwzVR6nmkwQlPrDMPa0HGvQk5HevePcTErP98SjE6MkyzACO+a96A5w9QF8X/
TiNTXI28ieTbRvog9q+oSW1GBEEDVOMrBm+WubA5NpNLc9HGrBYFhi4XYzXRM7RX
BLeqChZrcOP/79Ym9J22MfHLmZH1hdkWKY1ralIRAgMBAAGjUDBOMB0GA1UdDgQW
BBSLuYAnqAU5BRzxjU5EDzymFejknjAfBgNVHSMEGDAWgBSLuYAnqAU5BRzxjU5E
DzymFejknjAMBgNVHRMEBTADAQH/MA0GCSqGSIb3DQEBCwUAA4ICAQBvGo6DgfNc
/dQJ0ROZVtW4TPa0Pa+t8SydtPS3533wdZmqlDZWNhRPvENVKyowMhyDCmmdcTpn
X9FOdVMHf9WMeH1BDedjrqHpcLWHnOk/RMh6kJ95Qg15rdMTFUmLPv4XbWhh1Sof
SNZB2QXMG9G9Lu8Vg1DRrKs8TCcuaCZfnKERA4CiL0G6DTLd1os6QxZcOdrI+0PR
+9zErCvxTgRjO8cVLvLnkjBScrsEVXRX7KhHJQAv/jHlZHCW44IpXicEHGHGZn0F
QAHMb92NEaF10+GbZAoXi0Q2emDP+R4YDBNjG+wk2gcHLAnxDjbrhtPRrjUkPpiV
GdiNNFkplIJnCZepHiGc1Lhc477lP+MRFjfBdjAksPn4ECd902JdjUI4wFqnIU/y
mRp+TykVv9NmFWkhbf+T1gzt4S1eaWZT5BLp4x3jimOQvkEFXYLkfVwWRT/e3oJp
TtZYRyUgo/772y6/WwUoC7JVtDrAP4c78EyjY3tB2D5F4ruDQWN2s48ziautg7cM
KogCnn7keHB3k1PxmM+y8L4FLB8QG+54z9uMO9Q2bSIjB1PPHmY3GKLqOljmN3pz
Il6xnf3gHCMdRqP+++aZhIwtWGECD57n5CoP25iPelSVp9CmBKuu8HGFTEajofFK
B8nj9xIMi13bXa9zlWQquzo5WofYVSJxjw==
-----END CERTIFICATE-----
"""
      val sslConfig = SSLConfig(Some(certString), Some(privateKey), true, None)
      Mongo[IO](MongoConfig.basic(securedConnectionString).useSSL(sslConfig), blockingPool)
        .use(IO.pure)
        .attempt
        .map(_ must beRight)

    }
    "pkcs8" >>* {
      val privateKey = """
-----BEGIN CERTIFICATE-----
MIIEpDCCAowCCQCcDhAKaNop2zANBgkqhkiG9w0BAQsFADCBoDELMAkGA1UEBhMC
VVMxCzAJBgNVBAgMAkNPMSEwHwYDVQQKDBhJbnRlcm5ldCBXaWRnaXRzIFB0eSBM
dGQxMTAvBgNVBAsMKFRyYXZpcyB0ZXN0cyBmb3IgcXVhc2FyLWRhdGFzb3VyY2Ut
bW9uZ28xDTALBgNVBAMMBHJvb3QxHzAdBgkqhkiG9w0BCQEWEGJvdEBzbGFtZGF0
YS5jb20wHhcNMTkwOTAyMTIzNTAwWhcNMjIwNjIyMTIzNTAwWjCBhjELMAkGA1UE
BhMCVVMxCzAJBgNVBAgMAkNPMSEwHwYDVQQKDBhJbnRlcm5ldCBXaWRnaXRzIFB0
eSBMdGQxMTAvBgNVBAsMKFRyYXZpcyB0ZXN0cyBmb3IgcXVhc2FyLWRhdGFzb3Vy
Y2UtbW9uZ28xFDASBgNVBAMMC3Rlc3Rpbmcga2V5MIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEA1SF4dWUdM/vNrJ0R7OlnfygimlwCVASWZD+Esom0UPdH
uKdXPLlPATFcKJ+ChYk7MnHhNpb4gqLtMoiHqahqTj6z4MxSvb6+XI8BM/3dyetH
f4LYrJY1UNpj//0HsjLPh0mFlWCMxbMI0ojPIga7377WqngPpqMeIGxMcx7KgF8Z
9RspGDzf7vxMFM/uY7PB5lYID8Y2xro2thO+XSVyV2wmLwoFmajPomtLtVjUP6I7
qWwImcn6fBG/PmVrArqrqxae8WRHioEPjHhznhsAc5CKrWYMduuMienaj0Giqu3v
uq3YVV+5R+SN6Jg3TRaZ5j3nUoA9xSGR7AZ/RVarMQIDAQABMA0GCSqGSIb3DQEB
CwUAA4ICAQA4CX0gwXBFeQDOOGfaU0MFlSFl35ztnszybOsX7dIG9Cv0+j9MEXoP
yjLLSFMmMGKTWLOw69kIhrOfMC5tbgrHjYjiSc7WT39mCpxcNYOSvn4A+Fcz488w
I07Hr7etq29R38ICnCnpwx7GzW89e1s4IafL9GMh8hWWMI9Ig3dUPdxxIJpGQPtG
dv6m9mGkEQcvEXdLaLHEsEVMgSru1rkEJ8ciOrF8+v4+b/IzW1Lz18BgTjXSm4b5
8RQdnnhxz5MWr6urDTOgzR549P+j02yvcJrqiFJC5CbUedRWrdYG3atd6OhQQeYQ
BnebYiPxKpsOq4rNBz5RHHOMybitfk3hnhCHGb3rtkkRZ9yZkCalw4qU/30Kbkjl
zvpZU7HmnJ20mcDrerJUEMZN64sndQByHo22rIH1+b3F41VTyf/vpEnhxP0B9gLd
4bQJ90lQw0gooTUOvIJIhyPBq1+5E7CWLZsRT6gXqx2W1iobw+kVoAqxPlZ7wLHM
OSVPuFJTC88/nqHC08SG6dZ8jWHY0eWW3a1G8FhwvmIZ6MnskFQ9To+ylVObT2zB
+voA2aAAr9mnnC219udk236lqa7aaTWBhR79u2xU39dKicToiwta+jHbR8H8k/d5
RZR1+hOFgcehjPs+6wehxmXaWUJBZFbLdE6uQJljxxsmpHL/YfQ3Zw==
-----END CERTIFICATE-----
-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDVIXh1ZR0z+82s
nRHs6Wd/KCKaXAJUBJZkP4SyibRQ90e4p1c8uU8BMVwon4KFiTsyceE2lviCou0y
iIepqGpOPrPgzFK9vr5cjwEz/d3J60d/gtisljVQ2mP//QeyMs+HSYWVYIzFswjS
iM8iBrvfvtaqeA+mox4gbExzHsqAXxn1GykYPN/u/EwUz+5js8HmVggPxjbGuja2
E75dJXJXbCYvCgWZqM+ia0u1WNQ/ojupbAiZyfp8Eb8+ZWsCuqurFp7xZEeKgQ+M
eHOeGwBzkIqtZgx264yJ6dqPQaKq7e+6rdhVX7lH5I3omDdNFpnmPedSgD3FIZHs
Bn9FVqsxAgMBAAECggEAOitzdB8P1GNSZF1OSin+DG+I2jZokEPcRCot7oi1BTAo
jBZoMfOlw36UV6YJeqbU2FTKkhqsYwNS1+r5k+2abhahA6bgolcQH7UMMAOG6Bhm
IEA3IQuyK4ofpvuJgTJq/9KqOfXrFf5iVwvt9Kx/1HaCom44X1O/4juMthLeVCCh
E9YXSTRvBArUy5OZNXS2jB7RZNaaQACFTvUhodpgW1E8WzJBCRy2zPbRSPT8W/o6
W7LvZove7bivLzQUTrW7Ux4I/T/7f2ww3Cs7Bf+vHYEiWI6Bv6Y4JarZ8GfSBfh/
FteSsvwURHyD4RfZfBZudYmXzYU/4m5HgarB8DYnHQKBgQD0RZz3yQI6mZnVKS7v
0m4HnbiBnZxqFcfZph1cIqORVReruxAg9YMmtHcP4PcAynOzjvYIomo3o/sqGmzV
iRnkHRgDPw2oUm/HqJ/qpF8IRAUBw43YGm5VbR1UKLkWGmxdX/iBiNToakeXej+3
SbWPz8H76/LTym+Zj2dgTr+97wKBgQDfXRiPlDBLiGUg/82jBtU0xMvClxamFThq
qpHGlcLPTzhjbtul/fV8mGbmANw0OOBpXLFoaK3IocszxvBVM97WDhFw/D8Nt3pe
zGpqTS2FEi/7jrxrodw1GJsCyHXD+DCM2ojHIF8IGigDckG8T5PljK/LwUae5USG
0y3RZGtI3wKBgDOzVDDBs54RipSqPcv2nuEvybBtWoC9kWoIlag9kJ8fAn6Pfi9r
p1GW2qhatgzNzMVRWdOkBJU4ON3FcAmWFgvI+J8fRlW89PmuQAKavZ5zPxgLescl
ePrNuGpqoBoEh7EPsfpAqsdyTyrJbhjuGeaRU35thxy6ePpodrnUkRKzAoGAcsLa
Q6rXxXcmvnWvqftwv9P8/NW0BVER03wfufBoTytcxXnH0NlSr9l9xN7cRNiUNI4z
eHmQEG1QgiHMiXpFG6XOwEZZ/yot7VcJteRBZwWZMZ58oLBg+8bQnfVwzGdQSo5d
iDHRodAOjva9X4tjaaWHDSEYr7sMIlmOSO9XT/ECgYAFMDk6F7BYWEuB5VeIvukY
KsfB7WcvjsJVG1tB2lznAoDutVIrfznEwjbbDca4H+QYBOGV2JfSy2+JBrWIa9bS
U46clXbtkB/o4VlZ1kr8/3qF7yXSuDFcGGjFvrr1bPVjFA3kEFAsdyozyW0ClTyQ
v0c1P2rcMXUpZCx3UAuSBg==
-----END PRIVATE KEY-----
"""
      val certString = """
-----BEGIN CERTIFICATE-----
MIIGFTCCA/2gAwIBAgIJAJjzUnLXxIlrMA0GCSqGSIb3DQEBCwUAMIGgMQswCQYD
VQQGEwJVUzELMAkGA1UECAwCQ08xITAfBgNVBAoMGEludGVybmV0IFdpZGdpdHMg
UHR5IEx0ZDExMC8GA1UECwwoVHJhdmlzIHRlc3RzIGZvciBxdWFzYXItZGF0YXNv
dXJjZS1tb25nbzENMAsGA1UEAwwEcm9vdDEfMB0GCSqGSIb3DQEJARYQYm90QHNs
YW1kYXRhLmNvbTAeFw0xOTA5MDIxMjMyMTZaFw0yMjA2MjIxMjMyMTZaMIGgMQsw
CQYDVQQGEwJVUzELMAkGA1UECAwCQ08xITAfBgNVBAoMGEludGVybmV0IFdpZGdp
dHMgUHR5IEx0ZDExMC8GA1UECwwoVHJhdmlzIHRlc3RzIGZvciBxdWFzYXItZGF0
YXNvdXJjZS1tb25nbzENMAsGA1UEAwwEcm9vdDEfMB0GCSqGSIb3DQEJARYQYm90
QHNsYW1kYXRhLmNvbTCCAiIwDQYJKoZIhvcNAQEBBQADggIPADCCAgoCggIBAMkN
PLjWunQbKHJ93W+gT/UdpkYqp/N1bv8uxZiYhUjE+xm2dfRa8IJJRuxxx6RjJVhL
9WL4/pruGY0lrQPj4K/rqOWtGyzkLMQAvUBft0hgtVg9h/Eig7IiPQpyhBPQfu73
gSCIfviZX0jlcbC/HhhIO/wG185lMXG2Jpvztu6uFWISdoQn/WKnkzv6w9TcUTjS
ZEhiklXlRIj4S/65x7l0PJdjZM86BF1cWipHwcHwmkHJxVReZky0o5psRApLUBzy
aNKcmGBbLtdgdTQKTZhNhfYH1GxCqNopUeBE84/XvKx/8WFFf6LQ6e2lN1+qzM7U
0zg0iqYrYgyiHWWQJaIY0rksmfQWyaNimvNKRA1HfEzHW9aFmgkt2K4ncQ8BwNm7
dFPiToKQWCyK2vvYof/QUcb/f9JFlhcFaoHxdjvbV52pxnEEqBo8l9J94opRcx94
PuTByYYs+MLLO1vuqeilVAp+8bUfvQtsR/4Ri8vgd/Egy18nUPCDyLePj4WnuIKH
QEGQwzVR6nmkwQlPrDMPa0HGvQk5HevePcTErP98SjE6MkyzACO+a96A5w9QF8X/
TiNTXI28ieTbRvog9q+oSW1GBEEDVOMrBm+WubA5NpNLc9HGrBYFhi4XYzXRM7RX
BLeqChZrcOP/79Ym9J22MfHLmZH1hdkWKY1ralIRAgMBAAGjUDBOMB0GA1UdDgQW
BBSLuYAnqAU5BRzxjU5EDzymFejknjAfBgNVHSMEGDAWgBSLuYAnqAU5BRzxjU5E
DzymFejknjAMBgNVHRMEBTADAQH/MA0GCSqGSIb3DQEBCwUAA4ICAQBvGo6DgfNc
/dQJ0ROZVtW4TPa0Pa+t8SydtPS3533wdZmqlDZWNhRPvENVKyowMhyDCmmdcTpn
X9FOdVMHf9WMeH1BDedjrqHpcLWHnOk/RMh6kJ95Qg15rdMTFUmLPv4XbWhh1Sof
SNZB2QXMG9G9Lu8Vg1DRrKs8TCcuaCZfnKERA4CiL0G6DTLd1os6QxZcOdrI+0PR
+9zErCvxTgRjO8cVLvLnkjBScrsEVXRX7KhHJQAv/jHlZHCW44IpXicEHGHGZn0F
QAHMb92NEaF10+GbZAoXi0Q2emDP+R4YDBNjG+wk2gcHLAnxDjbrhtPRrjUkPpiV
GdiNNFkplIJnCZepHiGc1Lhc477lP+MRFjfBdjAksPn4ECd902JdjUI4wFqnIU/y
mRp+TykVv9NmFWkhbf+T1gzt4S1eaWZT5BLp4x3jimOQvkEFXYLkfVwWRT/e3oJp
TtZYRyUgo/772y6/WwUoC7JVtDrAP4c78EyjY3tB2D5F4ruDQWN2s48ziautg7cM
KogCnn7keHB3k1PxmM+y8L4FLB8QG+54z9uMO9Q2bSIjB1PPHmY3GKLqOljmN3pz
Il6xnf3gHCMdRqP+++aZhIwtWGECD57n5CoP25iPelSVp9CmBKuu8HGFTEajofFK
B8nj9xIMi13bXa9zlWQquzo5WofYVSJxjw==
-----END CERTIFICATE-----
"""
      val sslConfig = SSLConfig(Some(certString), Some(privateKey), true, None)
      Mongo[IO](MongoConfig.basic(securedConnectionString).useSSL(sslConfig), blockingPool)
        .use(IO.pure)
        .attempt
        .map(_ must beRight)

    }

  }
    "pkcs8 encrypted" >>* {
      val privateKey = """
-----BEGIN CERTIFICATE-----
MIIEpDCCAowCCQCcDhAKaNop2zANBgkqhkiG9w0BAQsFADCBoDELMAkGA1UEBhMC
VVMxCzAJBgNVBAgMAkNPMSEwHwYDVQQKDBhJbnRlcm5ldCBXaWRnaXRzIFB0eSBM
dGQxMTAvBgNVBAsMKFRyYXZpcyB0ZXN0cyBmb3IgcXVhc2FyLWRhdGFzb3VyY2Ut
bW9uZ28xDTALBgNVBAMMBHJvb3QxHzAdBgkqhkiG9w0BCQEWEGJvdEBzbGFtZGF0
YS5jb20wHhcNMTkwOTAyMTIzNTAwWhcNMjIwNjIyMTIzNTAwWjCBhjELMAkGA1UE
BhMCVVMxCzAJBgNVBAgMAkNPMSEwHwYDVQQKDBhJbnRlcm5ldCBXaWRnaXRzIFB0
eSBMdGQxMTAvBgNVBAsMKFRyYXZpcyB0ZXN0cyBmb3IgcXVhc2FyLWRhdGFzb3Vy
Y2UtbW9uZ28xFDASBgNVBAMMC3Rlc3Rpbmcga2V5MIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEA1SF4dWUdM/vNrJ0R7OlnfygimlwCVASWZD+Esom0UPdH
uKdXPLlPATFcKJ+ChYk7MnHhNpb4gqLtMoiHqahqTj6z4MxSvb6+XI8BM/3dyetH
f4LYrJY1UNpj//0HsjLPh0mFlWCMxbMI0ojPIga7377WqngPpqMeIGxMcx7KgF8Z
9RspGDzf7vxMFM/uY7PB5lYID8Y2xro2thO+XSVyV2wmLwoFmajPomtLtVjUP6I7
qWwImcn6fBG/PmVrArqrqxae8WRHioEPjHhznhsAc5CKrWYMduuMienaj0Giqu3v
uq3YVV+5R+SN6Jg3TRaZ5j3nUoA9xSGR7AZ/RVarMQIDAQABMA0GCSqGSIb3DQEB
CwUAA4ICAQA4CX0gwXBFeQDOOGfaU0MFlSFl35ztnszybOsX7dIG9Cv0+j9MEXoP
yjLLSFMmMGKTWLOw69kIhrOfMC5tbgrHjYjiSc7WT39mCpxcNYOSvn4A+Fcz488w
I07Hr7etq29R38ICnCnpwx7GzW89e1s4IafL9GMh8hWWMI9Ig3dUPdxxIJpGQPtG
dv6m9mGkEQcvEXdLaLHEsEVMgSru1rkEJ8ciOrF8+v4+b/IzW1Lz18BgTjXSm4b5
8RQdnnhxz5MWr6urDTOgzR549P+j02yvcJrqiFJC5CbUedRWrdYG3atd6OhQQeYQ
BnebYiPxKpsOq4rNBz5RHHOMybitfk3hnhCHGb3rtkkRZ9yZkCalw4qU/30Kbkjl
zvpZU7HmnJ20mcDrerJUEMZN64sndQByHo22rIH1+b3F41VTyf/vpEnhxP0B9gLd
4bQJ90lQw0gooTUOvIJIhyPBq1+5E7CWLZsRT6gXqx2W1iobw+kVoAqxPlZ7wLHM
OSVPuFJTC88/nqHC08SG6dZ8jWHY0eWW3a1G8FhwvmIZ6MnskFQ9To+ylVObT2zB
+voA2aAAr9mnnC219udk236lqa7aaTWBhR79u2xU39dKicToiwta+jHbR8H8k/d5
RZR1+hOFgcehjPs+6wehxmXaWUJBZFbLdE6uQJljxxsmpHL/YfQ3Zw==
-----END CERTIFICATE-----
-----BEGIN ENCRYPTED PRIVATE KEY-----
MIIE6TAbBgkqhkiG9w0BBQMwDgQIb2bBU/7KjsYCAggABIIEyEq6WaZ1bP+ipGhf
jbKC9YCNOvw3175tbDh/Xts2Hl0GRZX/MzLQvNwfqy2n3sfLRexf4wH1QaVfu9fS
SxZm6FC0GJlYVlz0cm0v9AcZ0LlsZugHDL5Y1yeB5KZoV7dvvJSUhWHCQX0Gqf2i
rGlgBixzCkRRkQ5CH6ehc1fSi7TWf02iRnOsfPKLEhoy2XJI12FiBiEDUZExuetL
52sWu1RiGx8+rmCbrAlf4QYeE7sRWSdftF709xWgRLfA63ScfFkCn0KBIQLuLRI/
uhL/DfH+w5pyiqjOdY0gldK49TUtb7UchSNQN82HXmwMYGQEdqdcwDHz7yJg06Ww
lWAyorqcDKLvCyQRNAEndhhTmz0+L0ct4+BpiDKnVbRlKrkisWLzWddUARXJEbnt
6U0KGO9QlUXpaX/D5qaM1F9o53GhlPK9iZwRCdjQ6f29tDThFCPIOLYv4jqsbucb
+v/MDYjg4cGzpdkHyEOSsMQhUxazYcMakBxP1HbFbvnbsqrzrtHnccp2DPCXiBsM
h+Kuar9Jn1InqDGwIG+b6s5VxczWmYtHF1lgMmmxuOXkZt6Sv9MzwQpGJBgo110G
lYVoW62ktBuAGAGM/vYRje5C4JOOIHKZ+MSIEHnqkO8oaZIjXi6A8C/Bp69L7Kmq
mPgcOmBZxfaouo71yUp/kZAdDb5q+TInbEd3wF5lymXtn//wm5nE+w0nwbOI9DwJ
QZH/G10H6b24uTgELnsWZKKypad88dSYc/P9+XmcU2MXWOgJW2tQjiSoKQiskV8X
uC9Li+m7CqlIoeSdUP7ybYTxd/gKrQ/aLuryOmg21345PrnnG0iKkxhtkjWzHYst
Lz2dd/0zlD1T059sEcgS+MD4zXRjVYrifFhG/KuqLOXsUy2XEAinTt2J3LYOEMR8
dkdCrQJ9eDanjuP5+bb1fa6nZj5wVOIdE5655EZLBjtBtWE7bMwTH9ASB/MGzvsE
iehKqHWwYDO4BZA7dWpsYNlFlzHa3v3tmcuLjjFhJPw3SjcagsuuPLHDW2C+O0Ks
W3Siua5BEmFn+duKb4Xd6rYnCboTf0GLpG4g44jiZgBO5TplGDEysJ4NZLgBKTTH
Uvn3YUFXSQhDjTe6JOG492imO7ObavF+8GMmMGEdB2Id6ib27/K1Bi8OValjYNyu
7wnSa+Fm042hbZoJmMSzM3lyNTjW1F13gTp76qD/Ii88dIqzcXzhL4hQG9RBsK/o
0nwJs9hvMBYvURY1WwaRmD2pS2rAG4TfA166BGOExlnbrPG8g9Eb3sA7aeAi8WuS
N0fCgX44ZyTSwyaCQV2/FMcm+8NylVTgIS0kSFzg/N0X7yM1vSx5HtWUb/OxwLo3
4dg3HIXcF+P27HW4hK9V23tbIE7x3jn/nbKXdoyzuaa2w4R9Y+GawP1CBi+f5X0H
Bz+PLbwFBaAv5ArUg9lxyU0zO3ZCZeopqrc2r/UXuyg4p5Ajl1cOnuwWAPyWYyi7
Rn4XGrGh2wnEZazOCECteWLDoOtuLx/ajEb4XroRAhw0VTsFPSL6g7fcZWVbrECA
t1r9bbwHRHFXmnJKWlb+10uL1wHy0JFsgs0t4H5KKU1imNVkXVBQWkfxBvH5O7w0
ZN2RnqR02V46WVjZOw==
-----END ENCRYPTED PRIVATE KEY-----
"""
      val certString = """
-----BEGIN CERTIFICATE-----
MIIGFTCCA/2gAwIBAgIJAJjzUnLXxIlrMA0GCSqGSIb3DQEBCwUAMIGgMQswCQYD
VQQGEwJVUzELMAkGA1UECAwCQ08xITAfBgNVBAoMGEludGVybmV0IFdpZGdpdHMg
UHR5IEx0ZDExMC8GA1UECwwoVHJhdmlzIHRlc3RzIGZvciBxdWFzYXItZGF0YXNv
dXJjZS1tb25nbzENMAsGA1UEAwwEcm9vdDEfMB0GCSqGSIb3DQEJARYQYm90QHNs
YW1kYXRhLmNvbTAeFw0xOTA5MDIxMjMyMTZaFw0yMjA2MjIxMjMyMTZaMIGgMQsw
CQYDVQQGEwJVUzELMAkGA1UECAwCQ08xITAfBgNVBAoMGEludGVybmV0IFdpZGdp
dHMgUHR5IEx0ZDExMC8GA1UECwwoVHJhdmlzIHRlc3RzIGZvciBxdWFzYXItZGF0
YXNvdXJjZS1tb25nbzENMAsGA1UEAwwEcm9vdDEfMB0GCSqGSIb3DQEJARYQYm90
QHNsYW1kYXRhLmNvbTCCAiIwDQYJKoZIhvcNAQEBBQADggIPADCCAgoCggIBAMkN
PLjWunQbKHJ93W+gT/UdpkYqp/N1bv8uxZiYhUjE+xm2dfRa8IJJRuxxx6RjJVhL
9WL4/pruGY0lrQPj4K/rqOWtGyzkLMQAvUBft0hgtVg9h/Eig7IiPQpyhBPQfu73
gSCIfviZX0jlcbC/HhhIO/wG185lMXG2Jpvztu6uFWISdoQn/WKnkzv6w9TcUTjS
ZEhiklXlRIj4S/65x7l0PJdjZM86BF1cWipHwcHwmkHJxVReZky0o5psRApLUBzy
aNKcmGBbLtdgdTQKTZhNhfYH1GxCqNopUeBE84/XvKx/8WFFf6LQ6e2lN1+qzM7U
0zg0iqYrYgyiHWWQJaIY0rksmfQWyaNimvNKRA1HfEzHW9aFmgkt2K4ncQ8BwNm7
dFPiToKQWCyK2vvYof/QUcb/f9JFlhcFaoHxdjvbV52pxnEEqBo8l9J94opRcx94
PuTByYYs+MLLO1vuqeilVAp+8bUfvQtsR/4Ri8vgd/Egy18nUPCDyLePj4WnuIKH
QEGQwzVR6nmkwQlPrDMPa0HGvQk5HevePcTErP98SjE6MkyzACO+a96A5w9QF8X/
TiNTXI28ieTbRvog9q+oSW1GBEEDVOMrBm+WubA5NpNLc9HGrBYFhi4XYzXRM7RX
BLeqChZrcOP/79Ym9J22MfHLmZH1hdkWKY1ralIRAgMBAAGjUDBOMB0GA1UdDgQW
BBSLuYAnqAU5BRzxjU5EDzymFejknjAfBgNVHSMEGDAWgBSLuYAnqAU5BRzxjU5E
DzymFejknjAMBgNVHRMEBTADAQH/MA0GCSqGSIb3DQEBCwUAA4ICAQBvGo6DgfNc
/dQJ0ROZVtW4TPa0Pa+t8SydtPS3533wdZmqlDZWNhRPvENVKyowMhyDCmmdcTpn
X9FOdVMHf9WMeH1BDedjrqHpcLWHnOk/RMh6kJ95Qg15rdMTFUmLPv4XbWhh1Sof
SNZB2QXMG9G9Lu8Vg1DRrKs8TCcuaCZfnKERA4CiL0G6DTLd1os6QxZcOdrI+0PR
+9zErCvxTgRjO8cVLvLnkjBScrsEVXRX7KhHJQAv/jHlZHCW44IpXicEHGHGZn0F
QAHMb92NEaF10+GbZAoXi0Q2emDP+R4YDBNjG+wk2gcHLAnxDjbrhtPRrjUkPpiV
GdiNNFkplIJnCZepHiGc1Lhc477lP+MRFjfBdjAksPn4ECd902JdjUI4wFqnIU/y
mRp+TykVv9NmFWkhbf+T1gzt4S1eaWZT5BLp4x3jimOQvkEFXYLkfVwWRT/e3oJp
TtZYRyUgo/772y6/WwUoC7JVtDrAP4c78EyjY3tB2D5F4ruDQWN2s48ziautg7cM
KogCnn7keHB3k1PxmM+y8L4FLB8QG+54z9uMO9Q2bSIjB1PPHmY3GKLqOljmN3pz
Il6xnf3gHCMdRqP+++aZhIwtWGECD57n5CoP25iPelSVp9CmBKuu8HGFTEajofFK
B8nj9xIMi13bXa9zlWQquzo5WofYVSJxjw==
-----END CERTIFICATE-----
"""
      val sslConfig = SSLConfig(Some(certString), Some(privateKey), true, Some("secret"))
      Mongo[IO](MongoConfig.basic(securedConnectionString).useSSL(sslConfig), blockingPool)
        .use(IO.pure)
        .attempt
        .map(_ must beRight)

    }

/*
  "tunnels" >> {
    "via key identity" >>* keyTunneledMongo.use(IO.pure).attempt.map(_ must beRight)
    "via user/password pair" >>* passwordTunneledMongo.use(IO.pure).attempt.map(_ must beRight)
  }
 */
}

object MongoSpec {
  import java.nio.file.{Files, Paths}

  import Mongo._
  import TunnelConfig.Pass._

  private lazy val blockingPool: BlockingContext = BlockingContext.cached("mongo-datasource")

  val dbs = List("A", "B", "C", "D")
  val cols = List("a", "b", "c", "d")
  val nonexistentDbs = List("Z", "Y")
  val nonexistentCols = List("z", "y")

  val host = Source.fromResource("mongo-host").mkString.trim

  val port: String = "27018"
  val securePort: String = "27019"

  val connectionString: String = s"mongodb://root:secret@${host}:${port}"
  val insecuredConnectionString: String = s"mongodb://${host}:${securePort}/?serverSelectionTimeoutMS=1000"
  val securedConnectionString: String = s"${insecuredConnectionString}&ssl=true"
  val connectionStringInvalidPort: String = s"mongodb://root:secret@${host}:27000/?serverSelectionTimeoutMS=1000"
  val aConnectionString: String = s"mongodb://aUser:aPassword@${host}:${port}/A.a"
  val invalidAConnectionString: String = s"mongodb://aUser:aPassword@${host}:${port}/"
  // Note, there is no collection, only db
  val bConnectionString: String = s"mongodb://bUser:bPassword@${host}:${port}/B"
  // And, there we have collection .b
  val bbConnectionString: String = s"mongodb://bUser:bPassword@${host}:${port}/B.b"

  val BatchSize: Int = 64

  def mkMongo: Resource[IO, Mongo[IO]] =
    Mongo[IO](MongoConfig.basic(connectionString).withPushdown(PushdownLevel.Full), blockingPool)

  def privateKey: IO[String] = IO.delay {
    val path = Paths.get("key_for_docker")
    new String(Files.readAllBytes(path))
  }

  val TunneledURL: String = "mongodb://root:secret@mng:27017"
  val TunnelUser: String = "root"
  val TunnelPassword: String = "root"
  val TunnelPassphrase: Option[String] = Some("passphrase")
  val TunnelHost: String = "localhost"
  val TunnelPort: Int = 22222

  def passwordTunneledMongo: Resource[IO, Mongo[IO]] =
    Mongo[IO](MongoConfig.basic(TunneledURL)
      .withPushdown(PushdownLevel.Full)
      .viaTunnel(TunnelConfig(TunnelHost, TunnelPort, TunnelUser, Some(Password(TunnelPassword)))),
      blockingPool)

  def keyTunneledMongo: Resource[IO, Mongo[IO]] =
    Resource.liftF(privateKey) flatMap { key =>
      Mongo[IO](MongoConfig.basic(TunneledURL)
        .withPushdown(PushdownLevel.Full)
        .viaTunnel(TunnelConfig(TunnelHost, TunnelPort, TunnelUser, Some(Identity(key, TunnelPassphrase)))),
        blockingPool)
    }

  def mkAMongo: Resource[IO, Mongo[IO]] =
    Mongo[IO](MongoConfig.basic(aConnectionString).withPushdown(PushdownLevel.Full), blockingPool)

  def mkInvalidAMongo: Resource[IO, Mongo[IO]] =
    Mongo[IO](MongoConfig.basic(invalidAConnectionString).withPushdown(PushdownLevel.Full), blockingPool)

  // create an invalid Mongo to test error scenarios, bypassing the ping check that's done in Mongo.apply
  def mkMongoInvalidPort: Resource[IO, Mongo[IO]] =
    for {
      client <- Mongo.mkClient[IO](
        MongoConfig(connectionStringInvalidPort, 64, PushdownLevel.Full, None, None),
        blockingPool)

      interpreter = new Interpreter(Version(0, 0, 0), "redundant", PushdownLevel.Full)
    } yield {
      new Mongo[IO](
        client, BatchSize.toLong, PushdownLevel.Full, interpreter, None)
    }

  def mkBMongo: Resource[IO, Mongo[IO]] =
    Mongo[IO](MongoConfig(bConnectionString, BatchSize, PushdownLevel.Full, None, None), blockingPool)

  def mkBBMongo: Resource[IO, Mongo[IO]] =
    Mongo[IO](MongoConfig(bbConnectionString, BatchSize, PushdownLevel.Full, None, None), blockingPool)

  def incorrectCollections: List[Collection] = {
    val incorrectDbStream =
      nonexistentDbs
        .map((dbName: String) => (colName: String) => Collection(Database(dbName), colName))
        .ap(cols ++ nonexistentCols)

    val incorrectColStream =
      nonexistentCols
        .map((colName: String) => (dbName: String) => Collection(Database(dbName), colName))
        .ap(dbs)
    incorrectDbStream ++ incorrectColStream
  }

  def correctCollections: List[Collection] =
    dbs.map((dbName: String) => (colName: String) => Collection(Database(dbName), colName)).ap(cols)

  def correctDbs: List[Database] =
    dbs.map(Database(_))

  def incorrectDbs: List[Database] =
    nonexistentDbs.map(Database(_))

  val incorrectPipeline: List[BsonDocument] = List(new BsonDocument("$incorrect", new BsonInt32(0)))

  def setupDB(): IO[Unit] = {
    val clientR =
      Mongo.mkClient[IO](MongoConfig(connectionString, BatchSize, PushdownLevel.Full, None, None), blockingPool)

    clientR.use(client => for {
      _ <- correctCollections.traverse { col => for {
        mongoCollection <- IO.delay(client.getDatabase(col.database.name).getCollection(col.name))
        _ <- singleObservableAsF[IO, Completed](mongoCollection.drop).attempt
        _ <- singleObservableAsF[IO, Completed](mongoCollection.insertOne(Document(col.name -> col.database.name)))
        // aUser:aPassword --> read only access to only one db, this means that aUser can't run listDatabases
        aDatabase <- IO.delay(client.getDatabase("A"))
        _ <- singleObservableAsF[IO, Document](aDatabase.runCommand[Document](Document(
          "createUser" -> "aUser",
          "pwd" -> "aPassword",
          "roles" -> List(Document("role" -> "read", "db" -> "A"))))).attempt
        // bUser:bPassword --> can do anything with "B" roles, but can't do anything with data level listCollections
        bDatabase <- IO.delay(client.getDatabase("B"))
        _ <- singleObservableAsF[IO, Document](bDatabase.runCommand[Document](Document(
          "createUser" -> "bUser",
          "pwd" -> "bPassword",
          "roles" -> List(Document("role" -> "userAdmin", "db" -> "B"))))).attempt
        } yield ()
      }
    } yield ())
  }
}
