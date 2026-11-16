## Consignment API

This is the API which accesses the TDR consignment database. 

### Schema
The schema of the database is managed in this [project](https://github.com/nationalarchives/tdr-consignment-api-data)

### Building locally
The auth utils and generated slick classes libraries are now stored in a private bucket in S3. This means you will need aws credentials to download the dependencies.
You need either a default profile set up or you need to set the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables. 

### Running locally

Set up the database
```
docker run --name postgres -p 5432:5432 -e POSTGRES_USER=tdr -e POSTGRES_PASSWORD=password -e POSTGRES_DB=consignmentapi -d postgres:17.2
git clone https://github.com/nationalarchives/tdr-consignment-api-data.git
cd tdr-consignment-api-data.git
sbt flywayMigrate
```

Run the api:

* From IntelliJ, run the ApiServer app
* Or from the command line, run `sbt run`

### Running the tests 

1. The repository and route tests use a docker container for the database. To run the tests locally, you will need to build this database.
```shell
docker build --no-cache -f Dockerfile-tests -t tests .
```

2. In order to run the tests run the command `sbt test`

* If you would like to run only one test suite, run the command `sbt "testOnly *{File name without .scala}"` e.g. `sbt "testOnly *ConsignmentRepositorySpec"`

#### Testing within IntelliJ

Running API tests within IntelliJ requires an additional VM option so that IntelliJ does not confuse the normal run config and test run config.
Within your test run configuration, set the VM of:

`-Dconfig.file=src/test/resources/application.conf`

If this is not set, you may see errors with the message: 

`Could not resolve substitution to a value: ${DB_PORT}`

#### Removing tests

If an API test is removed because it is no longer needed, you will need to modify the list of required status checks on Github.

These can be found in "Settings > Branches" under the `master` branch.

The branch status checks are run when a pull request is raised and will get stuck if it tries to run a check on a test that is no longer present. You can remove the test at this point and the checks will carry on to completion.   

#### Manual file checks progress / health check investigation

`src/test/scala/uk/gov/nationalarchives/tdr/api/manual/FileChecksProgressHealthCheck.scala` is a manual harness for investigating the
`healthcheck-full` timeouts seen on staging while a client polls the `fileChecks` progress of a consignment with a very large number of
`FileStatus` rows.

It runs the file checks progress query against a deployed environment and, at the same time, polls `/healthcheck` and `/healthcheck-full`
every second, so you can see whether and for how long the health check endpoints stop responding while the query is in flight. It prints a
baseline, a live timeline and a summary, then fails if any health check did not return a 200 while the query was running.

A single query is cheap (around 200ms for a consignment with 8000 files), so to reproduce the outage you need **concurrency and duration**
rather than a big consignment:

```shell
TDR_CONCURRENT_QUERIES=50 TDR_QUERY_REPEAT_SECONDS=120 sbt "testOnly *FileChecksProgressHealthCheck"
```

It is deliberately not named `*Spec` so the CI test matrix (one job per `*Spec.scala` file) does not pick it up, and it cancels itself
unless `TDR_CONSIGNMENT_ID` is set.

```shell
# Credentials for the environment's AWS account, used to read the Keycloak client secret from Parameter Store
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_SESSION_TOKEN=...
export TDR_CONSIGNMENT_ID=<a staging consignment with a lot of file statuses>
sbt "testOnly *FileChecksProgressHealthCheck"
```

Optional environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `TDR_ENVIRONMENT` | `staging` | Environment to point at. Used to derive the API, auth and SSM parameter paths |
| `TDR_API_URL` | `https://api.tdr-$TDR_ENVIRONMENT.nationalarchives.gov.uk` | API base URL |
| `TDR_AUTH_URL` | `https://auth.tdr-$TDR_ENVIRONMENT.nationalarchives.gov.uk` | Keycloak base URL |
| `TDR_QUERY` | `summary` | `summary` is the query the file checks page polls. `full` also loads every file and its statuses |
| `TDR_CONCURRENT_QUERIES` | `1` | How many clients send the query at the same time |
| `TDR_QUERY_REPEAT_SECONDS` | `0` | How long each client keeps re-sending the query. `0` sends it once |
| `TDR_ACCESS_TOKEN` | | Use an existing bearer token instead of fetching a service account one |
| `TDR_CLIENT_ID` | `tdr-backend-checks` | Keycloak client to authenticate as. It needs the `export` role to read any consignment |
| `TDR_CLIENT_SECRET` | | The client secret. If not set it is read from Parameter Store |
| `TDR_CLIENT_SECRET_PATH` | `/$TDR_ENVIRONMENT/keycloak/backend_checks_client/secret` | Parameter Store path for the client secret |
| `TDR_HEALTHCHECK_INTERVAL_SECONDS` | `1` | How often to poll the health check endpoints |
| `TDR_HEALTHCHECK_TIMEOUT_SECONDS` | `10` | Read timeout for a health check request |
| `TDR_QUERY_TIMEOUT_SECONDS` | `300` | Read timeout for the file checks progress query |
| `TDR_SLOW_QUERY_MILLIS` | `1000` | Queries slower than this are logged live. Other successful queries are only counted |
| `TDR_COOL_DOWN_SECONDS` | `15` | How long to carry on polling after the load has finished |

### Graphql Schema

We are now storing the current Graphql schema in the `schema.graphql` file in the root of the project. If you make changes to the API which cause a schema change, you will need to update this file with the contents of the newly generated schema, otherwise the test build will fail.

To generate the Graphql schema locally run the following command:

`sbt graphqlSchemaGen`

The generated schema file will be placed in the following location: `target/sbt-graphql/schema.graphql`. You can copy the contents of this file into `./schema.graphql` and commit the changes to allow the build to pass.

After this file is merged into master, it will only be used by the generated-graphql project when that project is next deployed.
In order to manually deploy the generated-graphql, follow [these instructions](https://github.com/nationalarchives/tdr-generated-graphql).
