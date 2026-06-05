(ns metabase.driver.databend
    "Driver for Databend databases"
    #_{:clj-kondo/ignore [:unsorted-required-namespaces]}
    (:require [clojure.java.jdbc :as jdbc]
      [clojure.string :as str]
      [clojure.tools.logging :as log]
      [honey.sql :as sql]
      [java-time :as t]
      [medley.core :as m]
      [metabase.config.core :as config]
      [metabase.driver :as driver]
      [metabase.util :as u]
      [metabase.driver.ddl.interface :as ddl.i]
      [metabase.driver.sql :as driver.sql]
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

(defn- describe-table-fields-via-sql
  ; DatabaseMetaData.getColumns() returns no rows against Databend, so query
  ; information_schema.columns directly for reliable field metadata.
  [database {:keys [name schema]}]
  (let [db-name (or (not-empty schema) (get-in database [:details :dbname] "default"))
        spec    (sql-jdbc.conn/connection-details->spec :databend (:details database))]
    (try
      (sql-jdbc.execute/do-with-connection-with-options
        :databend spec nil
        (fn [^Connection conn]
          (let [sql  (str "SELECT column_name, data_type, ordinal_position, is_nullable"
                          " FROM information_schema.columns"
                          " WHERE table_schema = ? AND table_name = ?"
                          " ORDER BY ordinal_position")
                stmt (doto (.prepareStatement conn sql)
                       (.setString 1 db-name)
                       (.setString 2 name))
                rset (.executeQuery stmt)]
            (loop [fields #{}]
              (if (.next rset)
                (let [col-name  (.getString rset "column_name")
                      data-type (.getString rset "data_type")
                      ord-pos   (.getInt    rset "ordinal_position")
                      is-null   (.getString rset "is_nullable")
                      safe-type (or data-type "")
                      upper-type (str/upper-case safe-type)]
                  (if (or (str/blank? safe-type)
                          (re-matches #"(?i)^AggregateFunction\(.+$" safe-type))
                    (recur fields)
                    (recur (conj fields
                                 {:name              col-name
                                  :database-type     safe-type
                                  :base-type         (or (sql-jdbc.sync/database-type->base-type :databend upper-type)
                                                         :type/*)
                                  :database-position (dec ord-pos)
                                  :nullable?         (= "YES" is-null)}))))
                fields)))))
      (catch Exception e
        (log/error e "describe-table-fields-via-sql failed for" name "in schema" db-name)
        #{}))))

(defmethod driver/describe-table :databend
  [_ database table]
  {:name   (:name table)
   :schema (:schema table)
   :fields (describe-table-fields-via-sql database table)})

(defmethod driver/describe-fields :databend
  ; Metabase v0.49+ uses describe-fields instead of describe-table for sync-fields.
  ; The sql-jdbc default calls getColumns() which returns nothing for Databend, so
  ; we override here to query information_schema.columns directly.
  [_driver database & {:keys [schema-names table-names]}]
  ; Early exit if caller explicitly requested an empty set of schemas or tables.
  (if (or (and schema-names (empty? schema-names))
          (and table-names (empty? table-names)))
    []
    (let [; Metabase stores schema as "" when none is set; treat blank the same as nil
          ; and fall back to the dbname from connection details (e.g. "gold").
          db-name (or (first (filter #(not (str/blank? %)) schema-names))
                      (get-in database [:details :dbname] "default"))
          spec    (sql-jdbc.conn/connection-details->spec :databend (:details database))
          tbl-clause (when (seq table-names)
                       (str " AND table_name IN ("
                            (str/join "," (repeat (count table-names) "?"))
                            ")"))
          sql    (str "SELECT table_schema, table_name, column_name, data_type,"
                      " ordinal_position, is_nullable"
                      " FROM information_schema.columns"
                      " WHERE table_schema = ?"
                      (or tbl-clause "")
                      " ORDER BY table_name, ordinal_position")
          params (into [sql db-name] (when (seq table-names) table-names))]
      (try
        (let [rows (jdbc/query spec params)]
          ; NOTE: return table-schema as nil. Databend's JDBC getTables() returns TABLE_SCHEM=""
          ; so Metabase stores schema=NULL in Postgres (JSON API serializes it as ""). Returning
          ; nil here causes Metabase to query WHERE schema IS NULL, which matches correctly.
          (vec (for [{:keys [table_name column_name data_type ordinal_position is_nullable]} rows
                     :let [safe-type  (or data_type "")
                           upper-type (str/upper-case safe-type)]
                     :when (not (or (str/blank? safe-type)
                                    (re-matches #"(?i)^AggregateFunction\(.+$" safe-type)))]
                 {:table-schema         nil
                  :table-name           table_name
                  :name                 column_name
                  :database-type        safe-type
                  :base-type            (or (sql-jdbc.sync/database-type->base-type :databend upper-type)
                                            :type/*)
                  :database-position    (dec (int ordinal_position))
                  :database-is-nullable (= "YES" is_nullable)
                  :pk?                  false})))
        (catch Exception e
          (log/error e "describe-fields :databend failed for schema" db-name)
          [])))))

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

(defmethod sql.qp/->honeysql [:databend :stddev]
           [driver [_ field]]
           [:'stddevPop (sql.qp/->honeysql driver field)])

(defmethod sql.qp/->honeysql [:databend :median]
           [driver [_ field]]
           [:'median (sql.qp/->honeysql driver field)])

(defn- args->float64
       [args]
       (map (fn [arg] [:'to_float64 (sql.qp/->honeysql :databend arg)]) args))

(defmethod sql.qp/->float :databend
           [_ value]
           [:'to_float64 value])
(defmethod sql.qp/->honeysql [:databend :substring]
           [driver [_ arg start length]]
           (let [str [:'toString (sql.qp/->honeysql driver arg)]]
                (if length
                  [:'substr str
                   (sql.qp/->honeysql driver start)
                   (sql.qp/->honeysql driver length)]
                  [:'substr str
                   (sql.qp/->honeysql driver start)])))

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
                              :upload-with-auto-pk             false
                              :describe-fields                 true}]

       (defmethod driver/database-supports? [:databend feature] [_driver _feature _db] supported?))

(defmethod sql-jdbc.sync/db-default-timezone :databend
           [_ spec]
           (let [sql (str "SELECT timezone() AS tz")
                 [{:keys [tz]}] (jdbc/query spec sql)]
                tz))

(defmethod driver/db-start-of-week :databend [_] :monday)

(defmethod ddl.i/format-name :databend [_ table-or-field-name]
  (str/replace table-or-field-name #"-" "_"))

(defmethod driver.sql/set-role-statement :databend
           [_ role]
           (format "SET ROLE %s;" role))
