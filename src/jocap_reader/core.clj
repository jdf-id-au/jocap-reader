(ns jocap-reader.core
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [tick.core :as t]
            [clojure.string :as s]
            [comfort.core :as c])
  (:import (com.linuxense.javadbf DBFDataType DBFReader)
           (java.sql Types)
           (java.io File)))

(def namer (comp keyword s/upper-case))

(def target-tables
  (into (sorted-set)
        [:A_ANEST
         :A_BLUT
         :A_CHECK
         :A_CPL
         :A_EREIG
         :A_FREE
         :A_GFM
         :A_MON
         :A_OFFBGA
         :A_ONBG2
         :A_ONBGA
         :A_OTHER1
         :A_OTHER2
         :A_PAT
         :A_PATEVE
         :A_PERF
         :PATGG
         :TRACK]))

(defn sorted-schema
  "Sort maps and retain table metadata. Optionally adjust types from java.sql/Types."
  ([schema] (sorted-schema schema nil))
  ([schema type-map]
   (into (sorted-map)
         (for [[table rows] schema]
           [table (with-meta
                    (into (sorted-map)
                          (if type-map
                            (for [[column details] rows]
                              [column (update details :type type-map)])
                            rows))
                    (meta rows))]))))

(def schema (-> "jocap-schema.edn" io/resource slurp edn/read-string sorted-schema))

(defn define
  "Display schema for table +- field."
  ([table] (define table nil))
  ([table field] (merge {:table (-> (get schema table) meta :title)}
                        (if field {:field (get-in schema [table field])}))))

(defn fields
  "List field names and English definitions."
  [table]
  (into (sorted-map) (for [[field {:keys [name unit storage comment]}] (get schema table)]
                       [field (when name (str name (when unit (str " (" unit ")"))))])))

#_ (define :A_PAT :PAT_NR)

(def units ; TODO might be useful later during parsing; might not reflect reality so don't change schema
  {"%" "%"
   "10^9/l" "/nL"
   "g/dl" "g/dL"
   "h" "h"
   "kPa" "kPa"
   "l" "L"
   "l/min" "L/min"
   "lpm" "L/min"
   "lpm/m²" "L/min/m^2"
   "mbar" "mbar"
   "meq/l" "mEq/L"
   "min" "min"
   "ml" "mL"
   "ml/min" "mL/min"
   "mlpm" "mL/min"
   "mmHg" "mmHg"
   "mmol/l" "mmol/L"
   "°C" "°C"})

(def types
  "Map DBFDataType to java.sql.Types guided by Microsoft ODBC behaviour:
    https://docs.microsoft.com/en-us/sql/odbc/microsoft/dbase-data-types
    https://docs.microsoft.com/en-us/sql/odbc/microsoft/microsoft-access-data-types

   In Jocap Data Dictionary:
   - NUMERIC have length up to 10: this appears to mean *characters* e.g. '-24.43' is 6
     This would be most faithfully converted into DECIMAL but this isn't what ODBC did.
   - LONG are all 4 bytes, i.e. 32 bits
   - TIMESTAMP and DATE are all 8
   - CHARACTER are up to 100
   - LOGICAL are all 1
   - MEMO has 12 instances, up to 64KB, refers to accompanying .fpt file

   In Access 2013 (Access -> ODBC names):
   - Number can be
    1 (Byte -> TINYINT), 2 (Integer -> SMALLINT), 4 (Long Integer -> INTEGER)
    4 (Single -> REAL), 8 (Double -> DOUBLE)
    12 (Decimal -> DECIMAL)
    [16 (Replication ID -> GUID) - Access 4.0 only]
    [? (Numeric -> NUMERIC) - Access 4.0 only]
    [8 (Large Number -> BIGINT) - Access >=2016]
   - Date/Time are 8
   - Short Text (formerly Text) are up to 255 characters [sic]
   - Yes/No are 1
   - Long Text (formerly Memo) are up to ~1GB
    https://support.office.com/en-us/article/set-the-field-size-ba65e5a7-2e6f-4737-8e72-36b93f966a33
    https://support.office.com/en-us/article/data-types-for-access-desktop-databases-df2b83ba-cef6-436d-b679-3418f622e482
   "
  {DBFDataType/NUMERIC Types/DOUBLE
   DBFDataType/CHARACTER Types/VARCHAR
   DBFDataType/DATE Types/DATE
   DBFDataType/TIMESTAMP Types/TIMESTAMP ; ref doesn't cover DBF ODBC for TIMESTAMP
   DBFDataType/MEMO Types/LONGVARCHAR ; TODO are these coming across? does lib look for FPT file?
   DBFDataType/LOGICAL Types/BIT
   DBFDataType/LONG Types/INTEGER}) ; ref doesn't cover DBF ODBC for LONG

(defn open-dbf
  "Opens dbf file and associated fpt (which contains MEMO data) if it exists."
  [^File file]
  (let [name (.getName file)
        stem (namer (subs name 0 (s/last-index-of name \.)))
        fpt (->> file .getCanonicalFile .getParent (c/ext namer "fpt") stem)
        r (DBFReader. (io/input-stream file))]
    (if fpt (.setMemoFile ^DBFReader r fpt) r)))

(defn dbf-row-seq
  "Lazy-load whole table. Make DATE and TIMESTAMP local."
  [^DBFReader dbfr]
  (let [fields (map #(.getField dbfr %) (range (.getFieldCount dbfr)))
        names (map #(keyword (.getName %)) fields)
        interpreters (map (fn [field] (condp = (.getType field) ; case needs compile-time literals!
                                        DBFDataType/DATE t/date
                                        DBFDataType/TIMESTAMP t/date-time
                                        ;DBFDataType/TIMESTAMP_DBASE7 t/date-time
                                        identity))
                          fields)
        interpret (fn [row] (into {} (for [[name interpret column]
                                           (map vector names interpreters row)]
                                       [name (some-> column interpret)])))
        extract (fn continue [reader]
                  (lazy-seq (if-let [row (.nextRecord reader)]
                              (cons (interpret row) (continue reader))
                              (.close reader))))] ; .close returns nil
    (extract dbfr)))