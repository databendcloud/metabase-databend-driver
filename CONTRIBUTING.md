## Getting started

Databend driver for Metabase is an open-source project,
and we welcome any contributions from the community.
Please share your ideas, contribute to the codebase,
and help us maintain up-to-date documentation.

* Please report any issues you encounter during operations.
* Feel free to create a pull request, preferably with a test or five.

## Setting up a development environment

### Requirements

* Clojure 1.11+
* OpenJDK 17
* Node.js 16.x
* Yarn

For testing: Docker Compose

Please refer to the extensive documentation available on the Metabase website: [Guide to writing a Metabase driver](https://www.metabase.com/docs/latest/developers-guide/drivers/start.html)

Databend driver's code should be inside the main Metabase repository checkout in `modules/drivers/databend` directory.

Additionally, you need to tweak Metabase `deps.edn` a bit.

The easiest way to set up a development environment is as follows (mostly the same as in the [CI](https://github.com/enqueue/metabase-databend-driver/blob/master/.github/workflows/check.yml)):

* Clone Metabase and Databend driver repositories
```bash
git clone https://github.com/metabase/metabase.git
cd metabase
git clone https://github.com/enqueue/metabase-databend-driver.git modules/drivers/databend
```

* Create custom Clojure profiles

```bash
mkdir -p ~/.clojure
cat modules/drivers/databend/.github/deps.edn | sed -e "s|PWD|$PWD|g" > ~/.clojure/deps.edn
```

Modifying `~/.clojure/deps.edn` will create two useful profiles: `user/databend` that adds driver's sources to the classpath, and `user/test` that includes all the Metabase tests that are guaranteed to work with the driver.

* Install the Metabase dependencies:

```bash
clojure -X:deps:drivers prep
```

* Build the frontend:

```bash
yarn && yarn build-static-viz
```

* Add /etc/hosts entry

Required for TLS tests.

```bash
sudo -- sh -c "echo 127.0.0.1 server.databendconnect.test >> /etc/hosts"
```

* Start Databend as a Docker container

```bash
docker compose -f modules/drivers/databend/docker-compose.yml up -d databend
```

Now, you should be able to run the tests:

```bash
DRIVERS=databend clojure -X:dev:drivers:drivers-dev:test:user/databend:user/test
```

you can see that we have our profiles `:user/databend:user/test` added to the command above, and with `DRIVERS=databend` we instruct Metabase to run the tests only for Databend.

NB: Omitting `DRIVERS` will run the tests for all the built-in database drivers.

If you want to run tests for only a specific namespace:

```bash
DRIVERS=databend clojure -X:dev:drivers:drivers-dev:test:user/databend:user/test :only metabase.driver.databend-test
```

## Building a jar

You need to add an entry for Databend in `modules/drivers/deps.edn`

```clj
{:deps
 {...
  metabase/databend {:local/root "databend"}
  ...}}
```

or just run this from the root Metabase directory, overwriting the entire file:

```bash
echo "{:deps {metabase/databend {:local/root \"databend\" }}}" > modules/drivers/deps.edn
```

Now, you should be able to build the final jar:

```bash
bin/build-driver.sh databend
```

As the result, `resources/modules/databend.metabase-driver.jar` should be created.

For smoke testing, there is a Metabase with the link to the driver available as a Docker container:

```bash
docker compose -f modules/drivers/databend/docker-compose.yml up -d metabase
```

It should pick up the driver jar as a volume.
