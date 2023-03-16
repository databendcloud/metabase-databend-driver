(ns metabase.driver.databend-test
    (:require [clojure.test :refer :all]
      [metabase.driver :as driver]
      [metabase.driver.sql-jdbc
       [connection :as sql-jdbc.conn]
       [sync :as sql-jdbc.sync]]
      [metabase.test :as mt]
      [metabase.test.data :as data]
      [metabase.test.data
       [datasets :as datasets]]
      [metabase.test.data.sql.ddl :as ddl]
      [metabase.util.honeysql-extensions :as hx]
      [metabase.query-processor-test :as qp.test]
      [honeysql.core :as hsql]
      [metabase.driver.sql.query-processor :as sql.qp]
      [metabase.test.data.sql :as sql.tx]
      [metabase.test.data.interface :as tx]
      [metabase.test.data.dataset-definitions :as dataset-defs]
      [clojure.string :as str]
      [metabase.driver.common :as driver.common]
      [metabase.driver.sql.util.unprepare :as unprepare]
      [metabase
       [models :refer [Table]]
       [sync :as sync]
       [util :as u]]
      [clojure.java.jdbc :as jdbc]
      [toucan.db :as db]
      [metabase.models
       [database :refer [Database]]])
    (:import [java.sql DatabaseMetaData Types Connection ResultSet]
      [java.time LocalTime OffsetDateTime OffsetTime ZonedDateTime]))

(deftest databend-server-timezone
         (mt/test-driver
           :databend
           (is (= "UTC"
                  (let [spec (sql-jdbc.conn/connection-details->spec :databend {})]
                       (metabase.driver/db-default-timezone :databend spec))))))