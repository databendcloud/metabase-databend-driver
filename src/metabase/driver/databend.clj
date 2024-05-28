(ns metabase.driver.databend
    "Driver for Databend databases"
    #_{:clj-kondo/ignore [:unsorted-required-namespaces]}
    (:require [clojure.java.jdbc :as jdbc]
      [clojure.string :as str]
      [clojure.tools.logging :as log]
      [honey.sql :as sql]
      [java-time :as t]
      [medley.core :as m]
      [metabase [config :as config] [driver :as driver] [util :as u]]
      [metabase.driver.ddl.interface :as ddl.i]
      [metabase.driver.sql.util :as sql.u]
      [metabase.driver.sql-jdbc [common :as sql-jdbc.common]
       [connection :as sql-jdbc.conn] [execute :as sql-jdbc.execute]
       [sync :as sql-jdbc.sync]]
      [metabase.driver.sql.query-processor :as sql.qp :refer [add-interval-honeysql-form]]
      [metabase.driver.sql.util.unprepare :as unprepare]
      [metabase.driver.sql-jdbc.sync.common :as common]
      [metabase.driver.sql-jdbc.execute.legacy-impl :as legacy]
      [metabase.driver.sql-jdbc.sync.interface :as i]
      [metabase.util.honey-sql-2 :as h2x]
      [metabase.util.date-2 :as u.date]
      [metabase.util.ssh :as ssh]
      [schema.core :as s])

    (:import
      [java.sql
       DatabaseMetaData
       ResultSet
       Connection
       ResultSetMetaData
       Types]
      [java.time
       LocalDate
       LocalDateTime
       LocalTime
       OffsetDateTime
       OffsetTime
       ZonedDateTime]
      java.util.Arrays))

(driver/register! :databend :parent :sql-jdbc)

(def ^:private database-type->base-type
  (sql-jdbc.sync/pattern-based-database-type->base-type
    [[#"ARRAY" :type/Array]
     [#"BOOLEAN" :type/Boolean]
     [#"DATETIME" :type/DateTime]
     [#"TIMESTAMP":type/DateTime]
     [#"DATE" :type/Date]
     [#"DECIMAL" :type/Decimal]
     [#"FLOAT" :type/Float]
     [#"DOUBLE" :type/Float]
     [#"TINYINT" :type/Integer]
     [#"SMALLINT" :type/Integer]
     [#"INT" :type/Integer]
     [#"BIGINT" :type/BigInteger]
     [#"VARCHAR" :type/Text]
     [#"VARIANT" :type/Text]
     [#"TINYINT UNSIGNED" :type/Integer]
     [#"SMALLINT UNSIGNED" :type/Integer]
     [#"INT UNSIGNED" :type/Integer]
     [#"BIGINT UNSIGNED" :type/BigInteger]
     ]))

(defmethod sql-jdbc.sync/database-type->base-type :databend
           [_ database-type]
           (let [base-type (database-type->base-type
                             (str/replace (name database-type)
                                          #"(?:Nullable)\((\S+)\)"
                                          "$1"))]
                base-type))

(def ^:private excluded-schemas #{"system" "information_schema" "INFORMATION_SCHEMA"})
(defmethod sql-jdbc.sync/excluded-schemas :databend [_] excluded-schemas)

(def ^:private default-connection-details
  {:classname "com.databend.jdbc.DatabendDriver", :subprotocol "databend", :user "root", :password "root", :dbname "default",:host "localhost", :port "8000", :ssl false})
(def ^:private product-name "metabase/1.4.1")
(defn- connection-details->spec* [details]
           ;; ensure defaults merge on top of nils
           (let [details (reduce-kv (fn [m k v] (assoc m k (or v (k default-connection-details))))
                                    default-connection-details
                                    details)
                 {:keys [classname subprotocol user password dbname host port ssl]} details]
                (->
                  {:classname    "com.databend.jdbc.DatabendDriver"
                   :subprotocol  "databend"
                   :dbname       (or dbname "default")
                   :subname      (str "//" host ":" port "/" dbname)
                   :password     (or password "")
                   :user         user
                   :ssl          (or (boolean ssl) false)
                   :product_name product-name}
                  (sql-jdbc.common/handle-additional-options details :separator-style :url))
                ))

(defmethod sql-jdbc.conn/connection-details->spec :databend
           [_ details]
           (connection-details->spec* details))

; Testing the databend database connection
(defmethod driver/can-connect? :databend
           [driver details]
           (if config/is-test?
             (try
               ;; Default SELECT 1 is not enough for Metabase test suite,
               ;; as it works slightly differently than expected there
               (let [spec  (sql-jdbc.conn/connection-details->spec driver details)
                     db    (or (:dbname details) (:db details) "default")]
                    (sql-jdbc.execute/do-with-connection-with-options
                      driver spec nil
                      (fn [^java.sql.Connection conn]
                          (let [stmt (.prepareStatement conn "SELECT count(*) > 0 FROM system.databases WHERE name = ?")
                                _    (.setString stmt 1 db)
                                rset (.executeQuery stmt)]
                               (when (.next rset)
                                     (.getBoolean rset 1))))))
               (catch Throwable e
                 (log/error e "An exception during Databend connectivity check")
                 false))
             ;; During normal usage, fall back to the default implementation
             (sql-jdbc.conn/can-connect? driver details)))


(def ^:private allowed-table-types
  (into-array String
              ["BASE TABLE" "TABLE" "VIEW" "MATERIALIZED VIEW" "MEMORY TABLE" "LOG TABLE"]))

(defn- tables-set
       [tables]
       (set
         (for [table tables]
              (let [remarks (:remarks table)]
                   {:name        (:table_name table)
                    :schema      (:table_schema table)
                    :description (when-not (str/blank? remarks) remarks)}))))

(defn- get-tables-from-metadata
       [metadata schema-pattern]
       (.getTables metadata                                 ; com.databend.jdbc.DatabendDatabaseMetaData#getTables
                   nil                                      ; catalog - unused in the source code there
                   schema-pattern
                   "%"                                      ; tablePattern "%" = match all tables
                   allowed-table-types))

(defn- get-tables-in-db
       [^DatabaseMetaData metadata db-name]
       ;; maybe snake-case is unnecessary here
       (let [db-name-snake-case (ddl.i/format-name :databend (or db-name "default"))]
            (tables-set
              (vec (jdbc/metadata-result
                     (get-tables-from-metadata metadata db-name-snake-case))))))

(defn- get-all-tables
       [metadata]
       (tables-set
         (filter
           #(not (contains? excluded-schemas (get % :table_schema)))
           (vec (jdbc/metadata-result
                  (get-tables-from-metadata metadata "%"))))))

(defn- ->spec
       [db]
       (if (u/id db)
         (sql-jdbc.conn/db->pooled-connection-spec db) db))

;; Strangely enough, the tests only work with :db keyword,
;; but the actual sync from the UI uses :dbname
(defn- get-db-name
       [db]
       (or (get-in db [:details :dbname])
           (get-in db [:details :db])))

(defmethod driver/describe-database :databend
           [_ db]
           (jdbc/with-db-metadata [metadata (->spec db)]
                                  (let [tables (if (get-in db [:details :scan-all-databases])
                                                 (get-all-tables metadata)
                                                 (get-tables-in-db metadata (get-db-name db)))]
                                       {:tables tables})))

(defmethod sql-jdbc.sync/database-type->base-type :databend [_ database-type]
           (database-type->base-type database-type))


(defmethod driver/describe-table :databend
           [_ database table]
           (let [table-metadata (sql-jdbc.sync/describe-table :databend database table)
                 filtered-fields (for [field (:fields table-metadata)
                                       :let [updated-field
                                             (update-in field [:database-type]
                                                        ;; Enum8(UInt8) -> Enum8
                                                        clojure.string/replace #"^(Enum.+)\(.+\)" "$1")]
                                       ;; Skip all AggregateFunction (but keeping SimpleAggregateFunction) columns
                                       ;; JDBC does not support that and it crashes the data browser
                                       :when (not (re-matches #"^AggregateFunction\(.+$"
                                                              (get field :database-type)))]
                                      updated-field)]
                (merge table-metadata {:fields (set filtered-fields)})))

(defn- to-start-of-year
       [expr]
       [:'to_start_of_year expr])

(defn- to-day-of-year
       [expr]
       [:'to_day_of_year expr])


(defn- to-start-of-week
       [expr]
       ;; The first day of a week can be Sunday or Monday, which is specified by the argument mode.
       ;; Here we use Sunday as default
       [:'to_start_of_week expr])

(defn- to-start-of-minute
       [expr]
       [:'to_start_of_minute expr])

(defn- to-start-of-hour
       [expr]
       [:'to_start_of_hour expr])

(defn- to-hour [expr] [:'to_hour expr])

(defn- to-minute [expr] [:'to_minute expr])


(defmethod sql.qp/date [:databend :day-of-week]
           [_ _ expr]
           (sql.qp/adjust-day-of-week :databend [:'to_day_of_week expr]))

(defn- to-day-of-month
       [expr]
       [:'to_day_of_month expr])

(defn- to-start-of-month
       [expr]
       [:'to_start_of_month expr])

(defn- to-start-of-quarter
       [expr]
       [:'to_start_of_quarter expr])

(defmethod sql.qp/date [:databend :default] [_ _ expr] expr)
(defmethod sql.qp/date [:databend :minute]
           [_ _ expr]
           (to-start-of-minute expr))

(defmethod sql.qp/date [:databend :minute-of-hour]
           [_ _ expr]
           (to-minute expr))
(defmethod sql.qp/date [:databend :hour] [_ _ expr] (to-start-of-hour expr))
(defmethod sql.qp/date [:databend :hour-of-day] [_ _ expr] (to-hour expr))
(defmethod sql.qp/date [:databend :day-of-month]
           [_ _ expr]
           (to-day-of-month expr))
(defmethod sql.qp/date [:databend :day-of-year]
           [_ _ expr]
           (to-day-of-year expr))
(defmethod sql.qp/date [:databend :month] [_ _ expr] (to-start-of-month expr))
(defmethod sql.qp/date [:databend :year] [_ _ expr] (to-start-of-year expr))

(defmethod sql.qp/date [:databend :week]
           [driver _ expr]
           (sql.qp/adjust-start-of-week driver to-start-of-week expr))
(defmethod sql.qp/date [:databend :quarter]
           [_ _ expr]
           (to-start-of-quarter expr))

(defmethod sql.qp/unix-timestamp->honeysql [:databend :seconds]
           [_ _ expr]
           (h2x/->datetime expr))

(defmethod unprepare/unprepare-value [:databend LocalDate]
           [_ t]
           (format "to_date('%s')" (t/format "yyyy-MM-dd" t)))

(defmethod unprepare/unprepare-value [:databend LocalTime]
           [_ t]
           (format "'%s'" (t/format "HH:mm:ss.SSS" t)))

(defmethod unprepare/unprepare-value [:databend OffsetTime]
           [_ t]
           (format "'%s'" (t/format "HH:mm:ss.SSSZZZZZ" t)))

; Converting OffsetDateTime datatype to SQL-style literal string
(defmethod unprepare/unprepare-value [:databend OffsetDateTime]
           [_ t]
           (format "'%s'" (u.date/format-sql (t/local-date-time t))))

(defmethod unprepare/unprepare-value [:databend LocalDateTime]
           [_ t]
           (format "'%s'" (t/format "yyyy-MM-dd HH:mm:ss.SSS" t)))

(defmethod unprepare/unprepare-value [:databend ZonedDateTime]
           [_ t]
           (format "'%s'" (t/format "yyyy-MM-dd HH:mm:ss.SSSZZZZZ" t)))

(defmethod sql.qp/->honeysql [:databend :log]
           [driver [_ field]]
           [:'log10 (sql.qp/->honeysql driver field)])

; call REGEXP_SUBSTR function when regex-match-first is called
(defmethod sql.qp/->honeysql [:databend :regex-match-first]
           [driver [_ arg pattern]]
           [:'extract (sql.qp/->honeysql driver arg) pattern])


;; metabase.query-processor-test.count-where-test
;; metabase.query-processor-test.share-test
(defmethod sql.qp/->honeysql [:databend :count-where]
           [driver [_ pred]]
           [:case
            [:> [:'count] 0]
            [:sum [:case (sql.qp/->honeysql driver pred) 1 :else 0]]
            :else nil])

(defmethod sql.qp/quote-style :databend [_] :mysql)

(defmethod sql.qp/add-interval-honeysql-form :databend
           [_ dt amount unit]
           (h2x/+ dt [:raw (format "INTERVAL %d %s" (int amount) (name unit))]))


(defmethod sql.qp/cast-temporal-byte [:databend :Coercion/ISO8601->Time]
           [_driver _special_type expr]
           (h2x/->timestamp expr))


(defmethod sql-jdbc.execute/read-column-thunk [:databend Types/TIMESTAMP_WITH_TIMEZONE]
           [_ ^ResultSet rs ^ResultSetMetaData _ ^Integer i]
           (fn []
               (when-let [s (.getString rs i)]
                         (u.date/parse s))))

(defmethod sql-jdbc.execute/read-column-thunk [:databend Types/TIME]
           [_ ^ResultSet rs ^ResultSetMetaData _ ^Integer i]
           (fn []
               (.getObject rs i OffsetTime)))

(defmethod sql-jdbc.execute/read-column-thunk [:databend Types/NUMERIC]
           [_ ^ResultSet rs ^ResultSetMetaData rsmeta ^Integer i]
           (fn []
               ; For some reason "count" is labeled as NUMERIC in the JDBC driver
               ; despite being just an UInt64, and it may break some Metabase tests
               (if (= (.getColumnLabel rsmeta i) "count")
                 (.getLong rs i)
                 (.getBigDecimal rs i))))

; Map databend data types to base types
(defmethod sql-jdbc.sync/database-type->base-type :databend [_ database-type]
           (database-type->base-type database-type))

; Concatenate the elements of an array based on array elemets type (coverting array data type to string type to apply filter on array data)
(defn is-string-array? [os]
      (if (= (type (first (vec os))) java.lang.String) (str "['" (clojure.string/join "','" os) "']") (str "[" (clojure.string/join "," os) "]")))

; Handle array data type
(defmethod metabase.driver.sql-jdbc.execute/read-column-thunk [:databend Types/ARRAY]
           [_ ^ResultSet rs _ ^Integer i]
           (fn []
               (def os (object-array (.getArray (.getArray rs i))))
               (is-string-array? os)))


(defmethod driver/display-name :databend [_] "Databend")

(doseq [[feature supported?] {:standard-deviation-aggregations true
                              :foreign-keys                    false
                              :set-timezone                    false
                              :convert-timezone                false
                              :test/jvm-timezone-setting       false
                              :connection-impersonation        false
                              :schemas                         true
                              :datetime-diff                   true
                              :upload-with-auto-pk             false}]

       (defmethod driver/database-supports? [:databend feature] [_driver _feature _db] supported?))

(defmethod sql-jdbc.sync/db-default-timezone :databend
           [_ spec]
           (let [sql (str "SELECT timezone() AS tz")
                 [{:keys [tz]}] (jdbc/query spec sql)]
                tz))

(defmethod driver/db-start-of-week :databend [_] :monday)

(defmethod ddl.i/format-name :databend [_ table-or-field-name]
  (str/replace table-or-field-name #"-" "_"))
