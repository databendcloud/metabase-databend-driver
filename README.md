# Metabase Databend Driver

## Installation

## Configuring

1. Once you've started up Metabase, open http://localhost:3000 , go to add a database and select "Databend".
2. You'll need to provide the Host/Port,  Database Name, Username and Password.

### Prerequisites

- [Leiningen](https://leiningen.org/)
- JDK 21 or newer (current builds target Metabase 0.56.x and above, which require Java 21+)

### Build from source

1. Clone and build metabase dependency jar.

   ```shell
   git clone https://github.com/metabase/metabase
   cd metabase
   clojure -X:deps prep
   cd modules/drivers
   clojure -X:deps prep
   cd ../..
   clojure -T:build uberjar
   ```

2. Clone metabase-databend-driver repo

   ```shell
   cd modules/drivers
   git clone https://github.com/databendcloud/metabase-databend-driver
   ```

3. Prepare metabase dependencies

   ```shell
   cp ../../target/uberjar/metabase.jar metabase-databend-driver/
   cd metabase-databend-driver
   mkdir repo
   mvn deploy:deploy-file -Durl=file:repo -DgroupId=com.databend -DartifactId=metabase-core -Dversion=1.40 -Dpackaging=jar -Dfile=metabase.jar
   ```

4. Build the jar

   ```shell
   LEIN_SNAPSHOTS_IN_RELEASE=true DEBUG=1 lein uberjar
   ```

   Notes:

   - Use JDK 21+ when compiling; newer Metabase builds (0.56.x and above) depend on Java 21 APIs and will fail on older JDKs.
   - This driver version is intended for Metabase 0.56.x and later. For older Metabase releases, use the matching driver version from the table below.

5. Let's assume we download `metabase.jar` from the [Metabase jar](https://www.metabase.com/docs/latest/operations-guide/running-the-metabase-jar-file.html) to `~/metabase/` and we built the project above. Copy the built jar to the Metabase plugins directly and run Metabase from there!

   ```shell
   cd ~/metabase/
   java -jar metabase.jar
   ```

You should see a message on startup similar to:

```
2019-05-07 23:27:32 INFO plugins.lazy-loaded-driver :: Registering lazy loading driver :databend...
2019-05-07 23:27:32 INFO metabase.driver :: Registered driver :databend (parents: #{:sql-jdbc}) 🚚
```

## Choosing the Right Version

| Metabase Release | Driver Version |
|------------------|----------------|
| 0.37.x           | 0.0.1          |
| 0.38.1+          | 0.0.2          |
| 0.41.2           | 0.0.3          |
| 0.41.3.1         | 0.0.4          |
| 0.42.x           | 0.0.5          |
| 0.44.x           | 0.0.6          |
| 0.47.7+          | 0.0.7          |
| 0.49.x           | 0.0.8          |
