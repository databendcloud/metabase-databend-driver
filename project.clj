(defproject metabase/databend-driver "0.0.3"
  :min-lein-version "2.5.0"

  :dependencies
  [[com.databend/databend-jdbc "0.0.7"]
   [clojure.java-time "0.3.2"]
   ]

  :repositories [["snapshots" {:sign-releases false
                               :url "https://repo.repsy.io/mvn/databend/maven-snapshots"
                               :username :env/REPSY_USER
                               :password :env/REPSY_PASSWORD}]
                 ["releases" {:sign-releases false
                              :url "https://repo.repsy.io/mvn/databend/maven"
                              :username :env/REPSY_USER
                              :password :env/REPSY_PASSWORD}]
                 ["project" "file:repo"]]

  :aliases
  {"test"       ["with-profile" "test"]}


:profiles {:provided
           {:dependencies [[com.databend/metabase-core "1.40"]]}
           :uberjar {:aot :all
                     :auto-clean     true
                     :target-path    "target/%s"
                     :uberjar-name   "databend.metabase-driver.jar"
                     :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}})
