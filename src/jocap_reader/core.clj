(ns jocap-reader.core
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [tick.core :as t]
            [clojure.string :as s]
            [comfort.core :as c]
            [clojure.core.match :refer [match]])
  (:import (com.linuxense.javadbf DBFDataType DBFReader)
           (java.sql Types)
           (java.io File)
           (java.time LocalDate)))

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
        fpt (->> file .getCanonicalFile .getParent (c/ext "fpt" namer) stem)
        r (DBFReader. (io/input-stream file))]
    (if fpt (.setMemoFile r fpt))
    r))

(defn open-table
  "Open table in path, e.g. (open-table \"/path/to/dbfs\" :A_PAT).
   Caller's responsibility to close."
  [path table-key]
  (->> path (c/ext "dbf" namer) table-key open-dbf))

(defn unless-nulls
  "Return memo as string unless it's all nulls."
  [memo]
  (if-not (every? (comp zero? int) memo) memo))

(defn dbf-row-seq
  "Lazy-load whole table. Make DATE and TIMESTAMP local."
  [^DBFReader dbfr]
  (let [fields (map #(.getField dbfr %) (range (.getFieldCount dbfr)))
        names (map #(keyword (.getName %)) fields)
        interpreters (map (fn [field] (condp = (.getType field) ; case needs compile-time literals!
                                        DBFDataType/DATE t/date
                                        DBFDataType/TIMESTAMP t/date-time
                                        DBFDataType/MEMO unless-nulls
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

(defmulti between? "Polymorphic bound-inclusive."
  ; Not protocol because "If you don’t own [the protocol or] the target type, you should only extend in app (not public lib) code, and expect to maybe be broken by either owner."
  (fn [x start end] (class x))) ; dispatch via isa?
(defmethod between? Number [x start end] (<= start x end))
(defmethod between? LocalDate [x start end] (and (t/<= start x) (t/<= x end)))

(defn try-int [s]
  (try (Integer/parseInt s)
       (catch NumberFormatException e #_(println (.getMessage e)))))

(defn string-match
  "Case-insensitive match. Ideally would be soft match."
  [x y]
  (= (s/lower-case x) (s/lower-case y)))

(defn case-filter
  "Return case filter which requires all listed conditions (most require A_PAT table).
   e.g. (case-filter [:surname \"Smith\"] [:dop 2020 4 3 - 2020 6 4])."
  [& conditions]
  (apply every-pred
    (for [condition conditions]
      (match [condition]
        ; ok to use * symbol etc because macro
        [[:procnum procnum]] #(some-> % :PAT_NR try-int (= procnum))
        [[:procnum starts-with *]] #(some-> % :PAT_NR (s/starts-with? (str starts-with)))
        ; inclusive to reduce surprise
        [[:procnum from - to]] #(some-> % :PAT_NR try-int (between? from to))
        [[:dob dob]] #(some-> % :GEB_DAT (= dob))
        [[:dob y m d]] #(some-> % :GEB_DAT (= (t/new-date y m d)))
        [[:dop dop]] #(some-> % :OP_DATUM (= dop))
        [[:dop y m d]] #(some-> % :OP_DATUM (= (t/new-date y m d)))
        [[:dop from - to]] #(some-> % :OP_DATUM (between? from to))
        [[:dop y1 m1 d1 - y2 m2 d2]] #(some-> % :OP_DATUM (between? (t/new-date y1 m1 d1)
                                                                    (t/new-date y2 m2 d2)))
        [[:mrn mrn]] #(some-> % :PAT_ID try-int (= mrn))
        [[:surname surname]] #(some-> % :NACHNAME (string-match surname))
        [[:surgeon surname]] #(some-> % :OPERATEUR (s/includes? surname))
        [[:anaesthetist surname]] #(some-> % :ANAESTH1 (s/includes? surname))
        [[:perfusionist surname]] #(some-> % :KARDIOT1 (s/includes? surname))))))

(defn extract-case
  "Eagerly load case from all tables."
  ; Unfortunately javadbf can't read CDX index files and I'm not going to implement it.
  [path procnum]
  (into (sorted-map)
    (for [[table-key file] (select-keys (c/ext "dbf" namer path) target-tables)]
      [table-key
       (with-open [table (open-dbf file)]
         (into []
           (->> table dbf-row-seq (filter (case-filter [:procnum procnum])))))])))
