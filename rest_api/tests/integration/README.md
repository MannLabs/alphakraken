# Integration tests of the REST API

The app runs in-process (`TestClient`) against a real MongoDB: routes, `shared.db` and the
queries are real, nothing is mocked.

```bash
docker run --rm -d --name kraken-integration-mongo -p 37017:27017 mongo:8.0-noble
python -m pytest rest_api/tests/integration
docker stop kraken-integration-mongo
```

- Without a MongoDB on `localhost:37017` (`INTEGRATION_MONGO_PORT`) the tests are skipped; in CI they fail.
- They use their own database `krakendb_integration`, dropped and seeded (`seed.py`) on every run.
- The yaml is the built-in one of `ENV_NAME=_test_`: one instrument, `_test1_`.

Not covered: the Docker image, and the Mongo users and roles of `misc/init-mongo.sh`.
