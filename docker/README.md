<!-- Copyright (c) 2026 IS4IT -->

# Test rig: SAP connector in a real midPoint server (Docker Compose)

Spins up a midPoint server backed by PostgreSQL 17 so the SAP ConnId connector can be
deployed and exercised through the midPoint GUI/REST. Adapted from the midpoint-lcm
rig; PostgreSQL bumped to 17.

## Quick start

```bash
# from the project root
docker compose -f docker/docker-compose.yml up -d      # first start (DB init + repo init + server)
docker/deploy-connector.sh                             # mvn package + deploy connector (+ JCo) + restart
```

- GUI: http://localhost:11080/midpoint — `administrator` / `T3stPw890uio`
- Logs: `docker compose -f docker/docker-compose.yml logs -f mp_server`
- DB:   `docker compose -f docker/docker-compose.yml exec mp_data psql -U midpoint` (password `db.secret.pw.007`)
- Tear down: `docker compose -f docker/docker-compose.yml down -v` (`-v` wipes volumes)

The GUI host port is **MP_PORT** (default 11080) and PostgreSQL is intentionally **not**
published on the host, so this rig can run alongside other midPoint/Postgres test stacks
without port clashes.

Versions and the GUI port are pinned in `docker/.env`: `MP_VER` (midPoint image tag),
`PG_VER` (PostgreSQL major version), `MP_PORT` (GUI host port).

## SAP JCo (required for the connector to load)

The connector bundle depends on SAP JCo, which is licensed and not redistributable, so
it is not part of the image or this repo. To use the connector you must supply the
**Linux** JCo build matching the container architecture and drop it into `docker/jco/`:

```
docker/jco/sapjco3.jar
docker/jco/libsapjco3.so
```

`deploy-connector.sh` copies the jar onto the classpath (`$MIDPOINT_HOME/lib`) and the
native library where `LD_LIBRARY_PATH` points (`$MIDPOINT_HOME/lib`). These files are
git-ignored. Without them midPoint still starts, but the SAP connector cannot be
instantiated (it would fail with a JCo `ClassNotFound`/`UnsatisfiedLinkError`).

Note: the macOS JCo (`sapjco3-darwinarm64-*`) used for `mvn test` does **not** work in the
Linux container — download the matching Linux build (e.g. `sapjco3-linuxx86_64-*` or
`-linuxaarch64-*` depending on the Docker VM architecture).

## Resource template (auto-imported from test.properties)

`connector-template.xml` is an **abstract resource template**. `deploy-connector.sh` merges
`src/test/resources/test.properties` into it and drops the result into
`$MIDPOINT_HOME/post-initial-objects/`; midPoint imports objects from that directory on
startup, so the restart at the end of the deploy makes the template appear in the repository.

The merge is generic — **every** key in `test.properties` becomes a `<cfg:KEY>` connector
configuration property, so the settings live in one place and are not duplicated in the XML:

- keys starting with `test.` are test-harness only and are skipped
- `r3name` maps to the connector's `systemId` property
- `password` is emitted as a `clearValue`, which midPoint encrypts on import
- a value containing `;` is multi-valued and becomes repeated `<cfg:KEY>` elements
  (e.g. `tables`, `tableParameterNames`)
- a generated property replaces the same-named default in the template; extra keys are appended
- all values are XML-escaped, so `WHERE` clauses containing `<`, `>` or `&` are safe

The `<cfg:…>` entries in the template (`useNativeNames`, `trace`, `traceLevel`, `tracePath`,
`baseAccountQuery`, `testBapiFunctionPermission`) are rig defaults; set the same key in
`test.properties` to override any of them. If a required connection key (`host`,
`systemNumber`, `r3name`, `client`, `user`, `password`) is missing, the template step is
skipped with a warning and the rest of the deploy proceeds.

The template has a fixed OID, so concrete test resources can inherit its connection config:

```xml
<resource oid="...">
    <name>my-sap-test</name>
    <super><resourceRef oid="f698ab61-55f4-4eec-bba4-81da4b9f52d8"/></super>
</resource>
```

If the template OID already exists in the repository, a re-import may be skipped, so after
changing `test.properties` either delete the resource template in midPoint and run the deploy
again, or `down -v` for a clean slate.

## Live tests (`SapResourceLiveTest`)

`mvn test` runs `SapResourceLiveTest`, which drives this rig over REST: each test creates one
concrete resource (inheriting the template) named `zz-test-sap-*` and asserts schema/query
behaviour. Cleanup happens at the **start** of a run, not the end — a run purges the
`zz-test-sap-*` resources left by the previous run and then **leaves its own**, so after `mvn test`
you can inspect and test them in the GUI. They are removed on the next run. The deployed template
(`SAPUM-J11`) and everything else are left untouched.

The tests need the rig running with the connector + JCo + template deployed (run `up -d` then
`deploy-connector.sh` once); when midPoint is unreachable they are skipped, so `mvn test` stays
green without the rig.

For a fully pristine midPoint — also wiping the repository DB, the deployed connector, JCo and the
template — do a full reset (the connector and template are stored in the volumes, so they must be
re-deployed afterwards):

```bash
docker compose -f docker/docker-compose.yml down -v
docker compose -f docker/docker-compose.yml up -d
docker/deploy-connector.sh
```

The per-run `zz-test-sap-*` purge already gives the tests a clean slate, so this full reset is only
needed when you want a pristine DB, not for normal test runs.

## How deployment works

Connector bundles and JCo are not bind-mounted (single-file/sub-dir bind mounts on
Colima/Docker Desktop on macOS are unreliable). `deploy-connector.sh` instead builds the
bundle, `docker cp`s it into `$MIDPOINT_HOME/icf-connectors/` (and JCo into
`$MIDPOINT_HOME/lib/`), renders + stages the resource template (see above), and restarts
`mp_server`. After the restart the connector is discovered and the template imported.
