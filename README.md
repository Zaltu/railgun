# Railgun - Powered by the Stars
Railgun is a streamlined data management system powered by STELLAR.

### In more than a line
Railgun is a wrapper for managing data within a database in a way that's intuitive for everyone. It includes both a web-based front-end and an accessible API for managing data, and particularly relations, without needing to deal with lower-level SQL implementation details. It's also infinitely scaleable (haha)!


## Features
- Use the integrated database or define any number of external database sources to wrap.
- Create and delete database structures (tables, columns) dynamically with no downtime.
- Define relations between entities without needing to deal with all the bs involved.
- Fully featured REST API
- Fully-featured [web-based UI](https://github.com/zaltu/railgun-react) (eternal WIP).
- Fully-featured [Python REST API wrapper](https://github.com/zaltu/railgun-api).
- Built for scaleability, spin up as many endpoints as needed.


## Quickstart
No pre-built images are available at the moment. Thankfully this is mostly python, so building is simple.
```bash
git clone https://github.com/zaltu/railgun && cd railgun
docker build --no-cache -t railgun:X.X.X -f deploy/Dockerfile.app .
docker build --no-cache -t stellardb:X.X.X -f deploy/Dockerfile.stellardb .
```
Adjust required volume mounts in `deploy/docker-compose.yml` and
```bash
docker compose -f deploy/docker-compose.yml up
```

## Deployment
Railgun is comprised of three containers:
- Railgun (requires building)
- redis
- PSQL (requires building)

### Building
From the root directory of the project:
```bash
docker build --no-cache -t railgun:X.X.X -f deploy/Dockerfile.app .
docker build --no-cache -t stellardb:X.X.X -f deploy/Dockerfile.stellardb .
```
To note, the only reason the StellarDB image deviates from the default `postgres` image is to create the initial user account. This will eventually be offloaded to the main app (TODO).


### docker-compose configuration
The following notes should be taken about the docker-compose file:  
- The `ports` settings of the `stellar` and `stellardb` services are not strictly necessary. They are referred to internally by service name within the docker network. They are exposed by default for the curious, but can be fully removed.

- The `RG_URL` env var in the `railgun_app` service is required. It is used to configure CORS access to the server. Include a comma-delimited list of any URLs+port numbers that may be used to connect to the server. (Obviously as this is for CORS, API access is not affected.)

- Aside from this, all volume mounts likely need to be adjusted for your system structure. See `Configuration Files` below.

- Do no change the `stellardb` service's environment variables.


### Volume Mounts (railgun)
#### db_secrets
`db_secrets` is the folder in which access parameters for all your databases are stored. Each database should have it's own file, with the following structure:
```
TODO
```
#### auth.secret
This should be a plaintext file containing only a random private key used for JWT signing.  
TODO this will eventually be automatically created on first startup.
#### files
`files` is the root folder for all files uploaded to your Railgun server. Make sure there is sufficient space for your needs.

### Volume Mounts (StellarDB)
#### pgdata
Location for StellarDB's PSQL data.
#### db_backups
Folder in which StellarDB backups will be placed when triggered (See Backups section).

### Running
```bash
docker compose -f deploy/docker-compose.yml up
```

**An account is created with the login `railgun`. Observe the logs in order to acquire the initial password for this account as it's generated dynamically on first setup.**


### Replication and Scaling Up
Railgun is built to be able to scale up indefinitely (haha). While nothing is truly indefinite, and you may end up being bottlenecked by DB restrictions, testing has show it to be have no ill effects with multiple hundreds of replications.  
Use of docker swarm or general k8 swarms is not documented officially (yet), but the simple way to spin up more instances is to spin up more `railgun`-only services as demonstrated in the provided `docker-compose.yml`. Make sure any new instances are also configured to be part of the correct docker network.

*It is also possible in this way to have multiple instances running, exposing different, potentially only partially overlapping databases.*


## Usage
Railgun and Stellar are wrapped by a FastApi web server to expose functionality. While it's possible to directly import `src.railgun.Railgun` and instantiate your own instance, it is not the expected way to operate and some behavior, particularly setup, authentication and file management may not function properly. Do at own risk.
### Exposed Endpoints
The following endpoints are exposed by the FastApi webserver for use.
- **/heartbeat**: check isalive
- **/login**: authenticate user and fetch access token/auth cookie
- **/create**: create new record
- **/read**: fetch information from DB
- **/update**: update existing record
- **/delete**: remove existing record
- **/batch**: run multiple CUD operations in a single call
- **/telescope**: fetch schema layout information
- **/stellar**: CUD operations on fields and entities (TODO - split into multiple endpoints)
- **/upload**: upload file to RG
- **/download**: download file from RG
- **/discharge**: static file mount

See COMPLETE_USAGE.md for detailed usage information on each endpoint.

## Authentication
Railgun uses signed JWT tokens for authentication, providing both a token directly for use in headers and an HTTP-only cookie.  
Passwords are encrypted on-server using `bcrypt`/blowfish encryption. *It is not secure to spin up a Railgun site over HTTP if you plan to access it from the outside.* Same for 99% of the internet of course, so that shouldn't come as a surprise.

## Backups
*This is subject to change*  
Railgun *does not back itself up automatically*. It provides a backup script that can be called from the host as desired and is put in a designated mounted volume. It is up to the user to determine the frequency of backups.  
To call the backup script, once the container is running:
```bash
docker exec -it deploy-stellardb-1 /opt/db_backup.sh
```

## Nomenclature and Data Management Features
Railgun is intended to act as a data management system. As a result, it is largely broken down into three primary chunks common to databases.  
A breakdown of the default data provided with Railgun can be found in DEFAULT_SCHEMA.md (TODO).
### Schemas
Schemas, in Railgun parlance, mean databases. Railgun can be configured to manage multiple difference database instances, and it is in fact encouraged to spin up a database separate from the provided `railgun_internal` database to avoid potential issues. Currently, new schemas need to be provided in a config file on app startup in order to be accessible. Seamless schema integration is a planned feature (TODO).  
Currently, only PSQL schemas are supported.
### Entities
Entities are traditional database tables. Railgun allows the dynamic creation and deletion of entities during runtime.
### Fields
Fields are traditional database columns. Railgun allows the dynamic creation and deletion of fields during runtime. The following field types are currently supported:
- **TEXT**  
Unicode string.
- **PASSWORD**  
Unicode string automatically stored in bcrypted form.
- **MEDIA**  
Uploaded file. Actually stored in DB as the relative server internal path.
- **INT**  
Standard integer.
- **FLOAT**
Standard float.
- **DATE**  
Date+Time with Timezone support.
- **JSON**  
Stored as JSONB where supported (PSQL).
- **BOOLEAN**  
SQL bools are weird, but here we are.
- **LIST**  
Enum-style type. Enforced application-side to allow changing "enum" values dynamically without making DB changes that could cause information loss.
- **ENTITY**  
Link any field to any single other entity of any number of customizable entity types. You only need to provide which types are valid for linking and the system will take care of the rest.
- **MUTLI_ENTITY**  
Link any field to any other entities of any number of customizable entity types. You only need to provide which types are valid for linking and the system will take care of the rest.
