(ns metabase.test.data.databend
    "Code for creating / destroying a Databend database from a `DatabaseDefinition`."
    (:require
      [metabase.driver.ddl.interface :as ddl.i]
      [metabase.driver.sql.util :as sql.u]
      [metabase.test.data
       [interface :as tx]
       [sql-jdbc :as sql-jdbc.tx]]
      [metabase.test.data.sql :as sql.tx]
      [metabase.test.data.sql-jdbc
       [execute :as execute]
       [load-data :as load-data]]))

(sql-jdbc.tx/add-test-extensions! :databend)

(def default-connection-params {:classname "com.databend.jdbc.DatabendDriver"
                                :subprotocol "databend"
                                :subname "//localhost:8000/default"
                                :user "root"
                                :password "root"
                                :ssl false
                                :product_name "metabase/1.1.2"})