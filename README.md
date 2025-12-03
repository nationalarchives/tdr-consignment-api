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

### Graphql Schema

We are now storing the current Graphql schema in the `schema.graphql` file in the root of the project. If you make changes to the API which cause a schema change, you will need to update this file with the contents of the newly generated schema, otherwise the test build will fail.

To generate the Graphql schema locally run the following command:

`sbt graphqlSchemaGen`

The generated schema file will be placed in the following location: `target/sbt-graphql/schema.graphql`. You can copy the contents of this file into `./schema.graphql` and commit the changes to allow the build to pass.

After this file is merged into master, it will only be used by the generated-graphql project when that project is next deployed.
In order to manually deploy the generated-graphql, follow [these instructions](https://github.com/nationalarchives/tdr-generated-graphql).

### Akka Licence  TODO REMOVE once moved to Pekko

The consignment-api makes use of a commercial Akka licence.

Akka provide:
* Licence Key: runtime check that valid licence
* Licence Token: build time access to the Akka binaries

Details for both can be found in the Akka account: https://account.akka.io/key (access required)

#### Licence Key

Required at runtime.

Stored in SSM parameter store in each of the TDR environment accounts (intg/staging/prod): `/{env}/akka/licence_key`

Requires manually adding to the SSM parameter

Referenced via ECS task definition secret in the `application.base.conf`:
```
    ...
    akka.licence-key = ${AKKA_LICENCE_KEY}
    ...
```

#### Licence Token

Required at build time to access the relevant Akka binaries.

Stored in SSM parameter store in the TDR management account: `/mgmt/akka/licence_token`

Requires manually adding to the SSM parameter

Referenced as a GitHub repository secret so available at build time: `AKKA_TOKEN`

Token value is used in the `akka.sbt` build file.

Repository secret set in the `tdr-terraform-github`

#### Application configuration

The token is stored in a SSM parameter in the TDR environment accounts (intg / staging / prod): {environment}/akka/licence_token

The token value is passed as an environment secret to the ECS task where is can be picked up in the application configuration file.

### Updating Licence Token

Both sets of SSM parameters need to be updated with the new licence token:

#### Running locally

The latest version of Akka can still be pulled down for local use. 

To ensure when building locally an error is not thrown set the environment variable `AKKA_TOKEN` with some placeholder value.

For example: `AKKA_TOKEN=dummyTokenValue`
