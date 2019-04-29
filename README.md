# Quasar Mongo [![Build Status](https://travis-ci.org/slamdata/quasar-datasource-mongo.svg?branch=master)](https://travis-ci.org/slamdata/quasar-datasource-mongo) [![Bintray](https://img.shields.io/bintray/v/slamdata-inc/maven-public/quasar-datasource-mongo.svg)](https://bintray.com/slamdata-inc/maven-public/quasar-datasource-mongo) [![Discord](https://img.shields.io/discord/373302030460125185.svg?logo=discord)](https://discord.gg/QNjwCg6)

## Usage

```sbt
libraryDependencies += "com.slamdata" %% "quasar-datasource-mongo" % <version>
```

Configuration

```json
{ "connectionString": <CONNECTION_STRING>,
  "batchSize": Int,
  "pushdownLevel": <"disabled"|"light"|"full">,
  "tunnelConfig": {
    host: String,
    port: Int,
    pass: <PASS>
  }
}
// PASS
{ "password": String } | { "key": String, "passphrase": String }
```

+ `connectionString` _must_ conform [Connection String Format](https://docs.mongodb.com/manual/reference/connection-string/)
+ `tunnelConfig` is optional
+ `pass.key` is content of private key file for ssh tunneling.

## Testing

The simplest way to test is using Nix system and run subset of `.travis.yml`:

```bash
$> docker run -d -p 127.0.0.1:27018:27017 --name mongodb -e MONGODB_ROOT_PASSWORD=secret bitnami/mongodb:4.1.4
$> docker-compose up -d
```

The second command starts two containers:
+ sshd with `root:root` with `22222` ssh port
+ mongo aliased as `mng` for sshd container.

(Unfortunately `docker-compose` doesn't work on Windows for me @cryogenian 29.04.2019)
