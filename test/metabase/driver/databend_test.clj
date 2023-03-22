(ns metabase.driver.databend-test
    "Tests for specific behavior of the Databend driver."
    #_{:clj-kondo/ignore [:unsorted-required-namespaces]}
    (:require [cljc.java-time.format.date-time-formatter :as date-time-formatter]
      [cljc.java-time.local-date :as local-date]
      [cljc.java-time.offset-date-time :as offset-date-time]
      [cljc.java-time.temporal.chrono-unit :as chrono-unit]
      [clojure.test :refer :all]
      [metabase.driver :as driver]
      [metabase.driver.common :as driver.common]
      [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
      [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync]
      [metabase.models.database :refer [Database]]
      [metabase.query-processor :as qp]
      [metabase.query-processor-test :as qp.test]
      [metabase.test :as mt]
      [metabase.test.data :as data]
      [metabase.test.data [interface :as tx]]
      [metabase.test.data.databend :refer [default-connection-params]]))

(deftest databend-connection-string
         (testing "connection with no additional options"
                  (is (= default-connection-params
                         (sql-jdbc.conn/connection-details->spec
                           :databend
                           {}))))
         (testing "custom connection with additional options"
                  (is (= (merge
                           default-connection-params
                           {:subname  "//mydatabend:9999/foo?connection_timeout=42"
                            :user     "bob"
                            :password "qaz"
                            :ssl      true})
                         (sql-jdbc.conn/connection-details->spec
                           :databend
                           {:host               "mydatabend"
                            :port               9999
                            :user               "bob"
                            :password           "qaz"
                            :dbname             "foo"
                            :additional-options "connection_timeout=42"
                            :ssl                true}))))
         (testing "nil dbname handling"
                  (is (= default-connection-params
                         (sql-jdbc.conn/connection-details->spec
                           :databend
                           {:dbname nil})))))

(deftest databend-boolean-result-metadata
         (mt/test-driver
           :databend
           (let [result (-> {:query "SELECT false, 123, true"} mt/native-query qp/process-query)
                 [[c1 _ c3]] (-> result qp.test/rows)]
                (testing "column should be of type :type/Boolean"
                         (is (= :type/Boolean (-> result :data :results_metadata :columns first :base_type)))
                         (is (= :type/Boolean (transduce identity (driver.common/values->base-type) [c1, c3])))
                         (is (= :type/Boolean (driver.common/class->base-type (class c1))))))))

(deftest databend-describe-database
         (let [[boolean-table]
               [
                {:description nil,
                 :name        "boolean_test",
                 :schema      "metabase_test"}]]
              (testing "scanning a single database"
                       (mt/with-temp Database
                                     [db {:engine  :databend
                                          :details {:dbname             "metabase_test"
                                                    :scan-all-databases nil}}]
                                     (let [describe-result (driver/describe-database :databend db)]
                                          (is (=
                                                {:tables
                                                 #{boolean-table}}
                                                describe-result))))
                       (testing "scanning all databases"
                                (mt/with-temp Database
                                              [db {:engine  :databend
                                                   :details {:dbname             "default"
                                                             :scan-all-databases true}}]
                                              (let [describe-result (driver/describe-database :databend db)]
                                                   ;; check the existence of at least some test tables here

                                                   (is (contains? (:tables describe-result)
                                                                  boolean-table))
                                                   ;; should not contain any Databend system tables
                                                   (is (not (some #(= (get % :schema) "system")
                                                                  (:tables describe-result))))
                                                   (is (not (some #(= (get % :schema) "information_schema")
                                                                  (:tables describe-result))))
                                                   (is (not (some #(= (get % :schema) "INFORMATION_SCHEMA")
                                                                  (:tables describe-result))))))))))
