(ns metabase.driver.databend
    "Driver for Databend databases"
    #_{:clj-kondo/ignore [:unsorted-required-namespaces]}
    (:require [clojure.java.jdbc :as jdbc]
      [clojure.string :as str]
      [clojure.tools.logging :as log]
      [honeysql [core :as hsql] [format :as hformat]]
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
      [metabase.mbql.schema :as mbql.s]
      [metabase.mbql.util :as mbql.u]
      [metabase.util.date-2 :as u.date]
      [metabase.util.honeysql-extensions :as hx]
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
(def ^:private product-name "metabase/1.4.0")

(defmethod sql-jdbc.conn/connection-details->spec :databend
           [_ details]
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


; Testing the databend database connection
(defmethod driver/can-connect? :databend [driver details]
           (let [connection (sql-jdbc.conn/connection-details->spec driver (ssh/include-ssh-tunnel! details))]
                (= 1 (first (vals (first (jdbc/query connection ["SELECT 1"])))))))


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
       (hsql/call :to_start_of_year (hsql/call :TO_DATETIME expr)))

(defn- to-day-of-year
       [expr]
       (hsql/call :to_day_of_year (hsql/call :TO_DATETIME expr)))


(defn- to-start-of-week
       [expr]
       ;; The first day of a week can be Sunday or Monday, which is specified by the argument mode.
       ;; Here we use Sunday as default
       (hsql/call :to_start_of_week expr))

(defn- to-start-of-minute
       [expr]
       (hsql/call :to_start_of_minute (hsql/call :TO_DATETIME expr)))

(defn- to-start-of-hour
       [expr]
       (hsql/call :to_start_of_hour (hsql/call :TO_DATETIME expr)))

(defn- to-hour [expr] (hsql/call :to_hour (hsql/call :TO_DATETIME expr)))

(defn- to-minute [expr] (hsql/call :to_minute (hsql/call :TO_DATETIME expr)))

(defn- to-day [expr] (hsql/call :to_date expr))

(defmethod sql.qp/date [:databend :day-of-week]
           [_ _ expr]
           (sql.qp/adjust-day-of-week :databend (hsql/call :to_day_of_week expr)))

(defn- to-day-of-month
       [expr]
       (hsql/call :to_day_of_month (hsql/call :TO_DATETIME expr)))

(defn- to-start-of-month
       [expr]
       (hsql/call :to_start_of_month (hsql/call :TO_DATETIME expr)))

(defn- to-start-of-quarter
       [expr]
       (hsql/call :to_start_of_quarter (hsql/call :TO_DATETIME expr)))

(defmethod sql.qp/date [:databend :default] [_ _ expr] expr)
(defmethod sql.qp/date [:databend :minute]
           [_ _ expr]
           (to-start-of-minute expr))

; Return an appropriate HoneySQL form for converting a Unix timestamp integer field or value to a proper SQL Timestamp.
;(defmethod sql.qp/unix-timestamp->honeysql [:databend :seconds] [_ _ expr] (hsql/call :to_timestamp expr))

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

(defmethod sql.qp/date [:databend :day] [_ _ expr] (to-day expr))
(defmethod sql.qp/date [:databend :week]
           [driver _ expr]
           (sql.qp/adjust-start-of-week driver to-start-of-week expr))
(defmethod sql.qp/date [:databend :quarter]
           [_ _ expr]
           (to-start-of-quarter expr))

;(defmethod sql.qp/unix-timestamp->honeysql [:databend :seconds]
;           [_ _ expr]
;           (hsql/call :TO_DATETIME expr))

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
           (hsql/call :log10 (sql.qp/->honeysql driver field)))

(defmethod hformat/fn-handler "quantile"
           [_ field p]
           (str "quantile(" (hformat/to-sql p) ")(" (hformat/to-sql field) ")"))

; call REGEXP_SUBSTR function when regex-match-first is called
(defmethod sql.qp/->honeysql [:databend :regex-match-first]
           [driver [_ arg pattern]]
           (let [arg-sql (hformat/to-sql (sql.qp/->honeysql driver arg))
                 pattern-sql (sql.u/escape-sql (sql.qp/->honeysql driver pattern) :ansi)
                 sql-string (str "REGEXP_SUBSTR(" arg-sql ", '" pattern-sql "')")]
                (hsql/raw sql-string)))


;; metabase.query-processor-test.count-where-test
;; metabase.query-processor-test.share-test
(defmethod sql.qp/->honeysql [:databend :count-where]
           [driver [_ pred]]
           (hsql/call :case
                      (hsql/call :>
                                 (hsql/call :count) 0)
                      (hsql/call :sum
                                 (hsql/call :case (sql.qp/->honeysql driver pred) 1.0 :else 0.0))
                      :else nil))

(defmethod sql.qp/quote-style :databend [_] :mysql)

(defmethod sql.qp/add-interval-honeysql-form :databend
           [_ dt amount unit]
           (hx/+ (hx/->timestamp dt)
                 (hsql/raw (format "INTERVAL %d %s" (int amount) (name unit)))))

;; The following lines make sure we call lowerUTF8 instead of lower
(defn- databend-like-clause
       [driver field value options]
       (if (get options :case-sensitive true)
         [:like field (sql.qp/->honeysql driver value)]
         [:like (hsql/call :lowerUTF8 field)
          (sql.qp/->honeysql driver (update value 1 str/lower-case))]))

(s/defn ^:private update-string-value :- mbql.s/value
        [value :- (s/constrained mbql.s/value #(string? (second %)) "string value") f]
        (update value 1 f))

(defmethod sql.qp/->honeysql [:databendd :starts-with]
           [driver [_ field value options]]
           (databend-like-clause driver
                                 (sql.qp/->honeysql driver field)
                                 (update-string-value value #(str % \%))
                                 options))

(defmethod sql.qp/->honeysql [:databend :contains]
           [driver [_ field value options]]
           (databend-like-clause driver
                                 (sql.qp/->honeysql driver field)
                                 (update-string-value value #(str \% % \%))
                                 options))

(defmethod sql.qp/->honeysql [:databend :ends-with]
           [driver [_ field value options]]
           (databend-like-clause driver
                                 (sql.qp/->honeysql driver field)
                                 (update-string-value value #(str \% %))
                                 options))


(defmethod sql.qp/cast-temporal-byte [:databend :Coercion/ISO8601->Time]
           [_driver _special_type expr]
           (hx/->timestamp expr))

;(defmethod sql-jdbc.execute/read-column-thunk [:databend Types/TIMESTAMP]
;           [_ ^ResultSet rs ^ResultSetMetaData _ ^Integer i]
;           (fn []
;               (let [r (.getObject rs i LocalDateTime)]
;                    (cond (nil? r) nil
;                          (= (.toLocalDate r) (t/local-date 1970 1 1)) (.toLocalTime r)
;                          :else r))))

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

(defmethod driver/supports? [:databend :standard-deviation-aggregations] [_ _] true)
(defmethod driver/supports? [:databend :set-timezone] [_ _] true)
(defmethod driver/supports? [:databend :foreign-keys] [_ _] false)
(defmethod driver/supports? [:databend :test/jvm-timezone-setting] [_ _] false)

(defmethod sql-jdbc.sync/db-default-timezone :databend
           [_ spec]
           (let [sql (str "SELECT timezone() AS tz")
                 [{:keys [tz]}] (jdbc/query spec sql)]
                tz))

(defmethod driver/db-start-of-week :databend [_] :monday)

(defmethod ddl.i/format-name :databend [_ table-or-field-name]
           (u/->snake_case_en table-or-field-name))
