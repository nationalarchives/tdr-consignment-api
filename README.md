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
docker run --name postgres -p 5432:5432 -e POSTGRES_USER=tdr -e POSTGRES_PASSWORD=password -e POSTGRES_DB=consignmentapi -d postgres:11.6
git clone https://github.com/nationalarchives/tdr-consignment-api-data.git
cd tdr-consignment-api-data.git
sbt flywayMigrate
```

Run the api:

* From IntelliJ, run the ApiServer app
* Or from the command line, run `sbt run`

### Testing within IntelliJ

Running API tests within IntelliJ requires an additional VM option so that IntelliJ does not confuse the normal run config and test run config.
Within your test run configuration, set the VM of:

`-Dconfig.file=src/test/resources/application.conf`

If this is not set, you may see errors with the message: 

`Could not resolve substitution to a value: ${DB_PORT}`

#### Graphql Schema

We are now storing the current Graphql schema in the `schema.graphql` file in the root of the project. If you make changes to the API which cause a schema change, you will need to update this file with the contents of the newly generated schema, otherwise the test build will fail.

To generate the Graphql schema locally run the following command:

`sbt graphqlSchemaGen`

The generated schema file will be placed in the following location: `target/sbt-graphql/schema.graphql`. You can copy the contents of this file into `./schema.graphql` and commit the changes to allow the build to pass.