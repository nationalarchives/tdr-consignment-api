# Copilot Instructions

## Build, Test, and Lint

```bash
# Compile
sbt compile

# Run all tests (requires the test Docker image to be built first)
docker build -f Dockerfile-tests -t tests .
sbt test

# Run a single test suite
sbt "testOnly *ConsignmentRepositorySpec"

# Format check (runs in CI)
sbt scalafmtCheckAll

# Apply formatting
sbt scalafmt

# Validate GraphQL schema hasn't changed
sbt 'graphqlValidateSchema "build" "consignmentApi"'

# Regenerate schema.graphql after API changes
sbt graphqlSchemaGen
# Then copy target/sbt-graphql/schema.graphql to ./schema.graphql
```

**AWS credentials** are required to download private dependencies (`consignment-api-db`, `tdr-auth-utils`) from S3. Set `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` or use a default AWS profile.

For IntelliJ test runs, add VM option: `-Dconfig.file=src/test/resources/application.conf`

## Architecture

This is a **Scala GraphQL API** built with:
- **Apache Pekko HTTP** — HTTP server and routing
- **Sangria** — GraphQL execution (schema, resolvers, middleware)
- **Slick** — Database access (PostgreSQL)
- **Circe** — JSON marshalling

Request flow: `Routes` → `GraphQLServer` → Sangria executor → `fields/*Fields.scala` resolvers → `service/*Service.scala` → `db/repository/*Repository.scala` → PostgreSQL

### Key components

- **`GraphQlTypes`** — assembles the root `Query` and `Mutation` types from all `fields/*.scala` files
- **`ConsignmentApiContext`** — the Sangria context object; holds the `Token` (auth) and all service instances; passed to every resolver via `ctx.ctx`
- **`ValidationAuthoriser`** — Sangria middleware that runs before each field; reads `AuthorisationTag`s attached to fields and calls their `validate` method
- **`ConsignmentStateValidator`** — same middleware pattern for consignment state checks (e.g. preventing double-uploads)
- **DB schema** — managed externally in [tdr-consignment-api-data](https://github.com/nationalarchives/tdr-consignment-api-data); Slick table classes are generated and published as the `uk.gov.nationalarchives:consignment-api-db` library

## Key Conventions

### GraphQL field definition pattern
Each domain area has a `fields/*Fields.scala` object. Fields attach validation tags directly:
```scala
Field(
  "startUpload",
  ...,
  tags = List(ValidateUserHasAccessToConsignment(StartUploadInputArg), ValidateNoPreviousUploadForConsignment)
)
```
Tags must extend `AuthorisationTag` (for auth) or `ConsignmentStateTag` (for state checks).

### Authorization tags
- Sync checks extend `SyncAuthorisationTag` and implement `validateSync`
- Async checks extend `AuthorisationTag` and implement `validateAsync`
- Throw `AuthorisationException` on failure; return `continue` on success

### Test structure — two distinct types
1. **Route specs** (`routes/*RouteSpec.scala`) — integration tests using a real PostgreSQL Testcontainer. Extend `TestContainerUtils`. Use `TestRequest.runTestRequest` to POST GraphQL queries loaded from JSON fixture files in `src/test/resources/json/`.
2. **Service specs** (`service/*ServiceSpec.scala`) — unit tests with Mockito. Extend `AnyFlatSpec with MockitoSugar with ResetMocksAfterEachTest`.
3. **Repository specs** (`db/repository/*RepositorySpec.scala`) — integration tests against the Testcontainer DB.

### JSON test fixtures
Route tests load queries and expected data from `src/test/resources/json/`. Files follow the naming convention `<operation>_<mutation|data>_<scenario>.json` (e.g. `addconsignment_mutation_alldata.json`, `addconsignment_data_all.json`).

### Test utilities
- `TestUtils(db)` — helper for direct SQL operations in tests (inserting seed data, deleting tables)
- `TestAuthUtils` — generates JWT tokens via `KeycloakMock` for different user roles (standard user, judgment user, TNA user, backend checks, export, etc.)
- `FixedTimeSource` / `FixedUUIDSource` — deterministic time and UUID values for service tests

### Database table names
Slick-generated table names use PascalCase with double-quoted identifiers in raw SQL: `"FileMetadata"`, `"ConsignmentStatus"`, etc.

### schema.graphql
`schema.graphql` in the repo root must be kept in sync with the code. CI validates it via `graphqlValidateSchema`. Update it manually after schema changes using `sbt graphqlSchemaGen`.

### Removing a test
If a test spec is deleted, remove it from the required GitHub branch status checks under **Settings → Branches → master**, otherwise PRs will block waiting for a check that no longer exists.
