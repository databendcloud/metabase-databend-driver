(ns metabase.driver.databend-describe-fields-test
  "Unit tests for driver/describe-fields :databend.
   These run without a live Databend server or the Metabase test framework."
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.test :refer :all]
            [metabase.driver :as driver]
            metabase.driver.databend
            [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]))

(deftest describe-fields-early-exit
  (testing "returns [] when schema-names is an empty collection"
    (is (= [] (driver/describe-fields :databend {} :schema-names []))))
  (testing "returns [] when table-names is an empty collection"
    (is (= [] (driver/describe-fields :databend {} :table-names [])))))

(deftest describe-fields-db-name-fallback
  (testing "uses :details :dbname when schema-names is not passed"
    (let [captured-params (atom nil)]
      (with-redefs [sql-jdbc.conn/connection-details->spec (fn [_ _] {})
                    jdbc/query (fn [_spec params] (reset! captured-params params) [])]
        (driver/describe-fields :databend {:details {:dbname "gold"}})
        (is (= "gold" (second @captured-params))))))
  (testing "uses :details :dbname when schema-names contains only blank/nil values"
    (let [captured-params (atom nil)]
      (with-redefs [sql-jdbc.conn/connection-details->spec (fn [_ _] {})
                    jdbc/query (fn [_spec params] (reset! captured-params params) [])]
        (driver/describe-fields :databend {:details {:dbname "gold"}} :schema-names ["" nil])
        (is (= "gold" (second @captured-params))))))
  (testing "uses first non-blank schema-name when provided"
    (let [captured-params (atom nil)]
      (with-redefs [sql-jdbc.conn/connection-details->spec (fn [_ _] {})
                    jdbc/query (fn [_spec params] (reset! captured-params params) [])]
        (driver/describe-fields :databend {:details {:dbname "other"}} :schema-names ["gold"])
        (is (= "gold" (second @captured-params)))))))

(deftest describe-fields-field-shape
  (testing "returns field maps with :table-schema nil and correct shape"
    (with-redefs [sql-jdbc.conn/connection-details->spec (fn [_ _] {})
                  jdbc/query (fn [_spec _params]
                               [{:table_name "sfdc_pipeline" :column_name "stage"
                                 :data_type "varchar" :ordinal_position 1 :is_nullable "YES"}
                                {:table_name "sfdc_pipeline" :column_name "amount"
                                 :data_type "double" :ordinal_position 2 :is_nullable "NO"}])]
      (let [result (driver/describe-fields :databend {:details {:dbname "gold"}})]
        (is (= 2 (count result)))
        (is (every? #(nil? (:table-schema %)) result)
            ":table-schema must be nil — Metabase stores schema=NULL in Postgres and matches via WHERE schema IS NULL")
        (is (every? #(false? (:pk? %)) result))
        (is (= {:table-schema nil
                :table-name "sfdc_pipeline"
                :name "stage"
                :database-type "varchar"
                :base-type :type/Text
                :database-position 0
                :database-is-nullable true
                :pk? false}
               (first (filter #(= "stage" (:name %)) result))))
        (is (= {:table-schema nil
                :table-name "sfdc_pipeline"
                :name "amount"
                :database-type "double"
                :base-type :type/Float
                :database-position 1
                :database-is-nullable false
                :pk? false}
               (first (filter #(= "amount" (:name %)) result))))))))

(deftest describe-fields-type-filtering
  (testing "excludes AggregateFunction columns"
    (with-redefs [sql-jdbc.conn/connection-details->spec (fn [_ _] {})
                  jdbc/query (fn [_spec _params]
                               [{:table_name "t" :column_name "agg_col"
                                 :data_type "AggregateFunction(sum, UInt64)"
                                 :ordinal_position 1 :is_nullable "YES"}
                                {:table_name "t" :column_name "normal_col"
                                 :data_type "varchar" :ordinal_position 2 :is_nullable "YES"}])]
      (let [result (driver/describe-fields :databend {:details {:dbname "gold"}})]
        (is (= 1 (count result)))
        (is (= "normal_col" (:name (first result)))))))
  (testing "excludes rows with nil data_type"
    (with-redefs [sql-jdbc.conn/connection-details->spec (fn [_ _] {})
                  jdbc/query (fn [_spec _params]
                               [{:table_name "t" :column_name "nil_col"
                                 :data_type nil :ordinal_position 1 :is_nullable "YES"}
                                {:table_name "t" :column_name "good_col"
                                 :data_type "bigint" :ordinal_position 2 :is_nullable "YES"}])]
      (let [result (driver/describe-fields :databend {:details {:dbname "gold"}})]
        (is (= 1 (count result)))
        (is (= "good_col" (:name (first result))))))))

(deftest describe-fields-table-names-filter
  (testing "passes table names as IN-clause params when table-names provided"
    (let [captured-params (atom nil)]
      (with-redefs [sql-jdbc.conn/connection-details->spec (fn [_ _] {})
                    jdbc/query (fn [_spec params] (reset! captured-params params) [])]
        (driver/describe-fields :databend {:details {:dbname "gold"}}
                                :schema-names ["gold"]
                                :table-names ["orders" "customers"])
        (let [params @captured-params]
          (is (= 4 (count params)) "sql + schema + 2 table-names = 4 params")
          (is (= "gold" (nth params 1)))
          (is (= "orders" (nth params 2)))
          (is (= "customers" (nth params 3)))))))
  (testing "omits table-name params when table-names is nil"
    (let [captured-params (atom nil)]
      (with-redefs [sql-jdbc.conn/connection-details->spec (fn [_ _] {})
                    jdbc/query (fn [_spec params] (reset! captured-params params) [])]
        (driver/describe-fields :databend {:details {:dbname "gold"}} :schema-names ["gold"])
        (is (= 2 (count @captured-params)) "sql + schema only = 2 params")
        (is (= "gold" (nth @captured-params 1)))))))

(deftest describe-fields-database-supports
  (testing ":describe-fields feature returns true so Metabase routes sync through our implementation"
    (is (true? (driver/database-supports? :databend :describe-fields nil)))))

(deftest describe-fields-exception-handling
  (testing "returns [] and does not rethrow when jdbc/query throws"
    (with-redefs [sql-jdbc.conn/connection-details->spec (fn [_ _] {})
                  jdbc/query (fn [_spec _params] (throw (Exception. "connection refused")))]
      (let [result (driver/describe-fields :databend {:details {:dbname "gold"}})]
        (is (= [] result)))))
  (testing "falls back to :details :dbname = \"default\" when :dbname key is absent"
    (let [captured-params (atom nil)]
      (with-redefs [sql-jdbc.conn/connection-details->spec (fn [_ _] {})
                    jdbc/query (fn [_spec params] (reset! captured-params params) [])]
        (driver/describe-fields :databend {:details {}})
        (is (= "default" (second @captured-params)))))))
