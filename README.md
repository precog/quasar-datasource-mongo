# Quasar Mongo [![Discord](https://img.shields.io/discord/373302030460125185.svg?logo=discord)](https://discord.gg/QNjwCg6)

## Usage

```sbt
libraryDependencies += "com.precog" %% "quasar-datasource-mongo" % <version>
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

The simplest way to test is using Nix system and run subset of `.travis.yml`. One time only, generate an ssh key:

```bash
$> """ssh-keygen -t rsa -N "passphrase" -f key_for_docker -m PEM""",
```

Then, when you want to test, run this:

```bash
$> docker swarm init
$> docker stack deploy -c docker-compose.yml teststack
```

It starts multiple containers:
+ sshd with `root:root` with `22222` ssh port
+ mongo aliased as `mng` for sshd container.
+ plain mongo on port 27018
+ SSL-enabled mongo on port 27019
+ SSL-enabled mongo with client-required key on port 27020

You can stop it afterwards with

```bash
$> docker stack rm teststack
$> docker swarm leave --force
```

(Unfortunately `docker-compose` doesn't work on Windows for me @cryogenian 29.04.2019)
