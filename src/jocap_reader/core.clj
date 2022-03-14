(ns jocap-reader.core
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [tick.core :as t]
            [clojure.string :as s]
            [comfort.io :as c]
            [clj-fuzzy.levenshtein :as lev]
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
                        (when field {:field (get-in schema [table field])}))))

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
   DBFDataType/MEMO Types/LONGVARCHAR
   DBFDataType/LOGICAL Types/BIT
   DBFDataType/LONG Types/INTEGER}) ; ref doesn't cover DBF ODBC for LONG

(defn open-dbf
  "Opens dbf file and associated fpt (which contains MEMO data) if it exists."
  [^File file]
  (let [name (.getName file)
        stem (namer (subs name 0 (s/last-index-of name \.)))
        fpt (->> file .getCanonicalFile .getParent (c/ext "fpt" namer) stem)
        r (DBFReader. (io/input-stream file))]
    (when fpt (.setMemoFile r fpt))
    r))

(defn open-table
  "Open table in path, e.g. (open-table \"/path/to/dbfs\" :A_PAT).
   Caller's responsibility to close."
  [path table-key]
  (->> path (c/ext "dbf" namer) table-key open-dbf))

(defn unless-nulls
  "Return memo as string unless it's all nulls."
  [memo]
  (when-not (every? (comp zero? int) memo) memo))

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

(defn ci
  "Case-insensitive comparison using f. Idiosyncratic arg order to make thread macro tidier."
  [x f y]
  (f (s/lower-case x) (s/lower-case y)))

(defn full-name
  [{:keys [VORNAME NACHNAME]}]
  (str VORNAME \space NACHNAME))

(defn short-lev
  [x y]
  (>= 2 (lev/distance x y)))

(defn multi-field
  "Return function to make a list of values from consecutively numbered fields. (like OP_ART)"
  [prefix number]
  (apply juxt (map #(keyword (str (name prefix) \_ %)) (range 1 (inc number)))))

(defn all-of
  [row prefix number]
  (->> ((multi-field prefix number) row)
       (remove s/blank?)
       (s/join \newline)))

(defn case-filter
  "Return case filter which requires all listed conditions.
   All except :procnum refer to A_PAT table.
   e.g. (case-filter [:surname \"Smith\"] [:dop 2020 4 3 :to 2020 6 4])."
  [& conditions]
  (apply every-pred
    (for [condition conditions]
      (match [condition]
        [[:procnum procnum]] #(some-> % :PAT_NR try-int (= procnum))
        [[:procnum starts-with :all]] #(some-> % :PAT_NR (s/starts-with? (str starts-with)))
        ; inclusive to reduce surprise
        [[:procnum from :to to]] #(some-> % :PAT_NR try-int (between? from to))
        [[:dob dob]] #(some-> % :GEB_DAT (= dob))
        [[:dob y m d]] #(some-> % :GEB_DAT (= (t/new-date y m d)))
        [[:dop dop]] #(some-> % :OP_DATUM (= dop))
        [[:dop y m d]] #(some-> % :OP_DATUM (= (t/new-date y m d)))
        [[:dop from :to to]] #(some-> % :OP_DATUM (between? from to))
        [[:dop y1 m1 d1 :to y2 m2 d2]] #(some-> % :OP_DATUM (between? (t/new-date y1 m1 d1)
                                                                      (t/new-date y2 m2 d2)))
        [[:mrn mrn]] #(some-> % :PAT_ID try-int (= mrn))
        [[:surname surname]] #(some-> % :NACHNAME (ci = surname))
        [[:name name]] #(-> % full-name (ci short-lev name))
        [[:surgeon surname]] #(some-> % :OPERATEUR (ci s/includes? surname))
        [[:anaesthetist surname]] #(some-> % :ANAESTH1 (ci s/includes? surname))
        [[:perfusionist surname]] #(some-> % :KARDIOT1 (ci s/includes? surname))
        [[:diagnosis diagnosis]] #(-> % (all-of :DIAGNOSE 6) (ci s/includes? diagnosis))
        [[:operation operation]] #(-> % (all-of :OP_ART 6) (ci s/includes? operation))))))

(defn find-cases
  "Lazily find cases at path matching conditions. Return matching A_PAT rows."
  [path & conditions]
  (some->> path (c/ext "dbf" namer) :A_PAT open-dbf dbf-row-seq
           (filter (apply case-filter conditions))))

(defn extract
  "Lazily load rows from all tables pertaining to cases (A_PAT rows or int procnums) at path."
  ; Unfortunately javadbf can't read CDX index files and I'm not going to implement it.
  ; Probably no benefit from scanning multiple dbfs at once? Could determine empirically?
  ; Relies on completed dbf-row-seq closing its file.
  [path cases]
  (let [files (select-keys (c/ext "dbf" namer path) (disj target-tables :PATGG :TRACK))
        procnums (set (condp #(%1 %2) (first cases)
                        map? (map (comp try-int :PAT_NR) cases)
                        integer? cases))]
    (for [[table-key file] files
          :let [table (open-dbf file)]]
      [table-key
       (for [{:keys [PAT_NR] :as row} (dbf-row-seq table)
             :when (contains? procnums (try-int PAT_NR))]
         row)])))
