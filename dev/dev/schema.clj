(ns dev.schema
  (:require [clojure.string :as s]
            [clojure.java.io :as io]
            [jocap-reader.core :as j]
            [comfort.core :as c])
  (:import (org.apache.pdfbox.pdmodel PDDocument)
           (org.apache.pdfbox.text PDFTextStripper)
           (com.linuxense.javadbf DBFDataType DBFReader)
           (java.sql Types)))

(defn get-dd [filename]
  (with-open [pdf (->> filename io/resource io/file PDDocument/load)]
    (.getText (PDFTextStripper.) pdf)))

(defn dd-tables [dd]
  (s/split dd #"Table "))

(def types
  "Map DBFDataType to java.sql.Types guided by Microsoft ODBC behaviour:
    https://docs.microsoft.com/en-us/sql/odbc/microsoft/dbase-data-types
    https://docs.microsoft.com/en-us/sql/odbc/microsoft/microsoft-access-data-types

   In Jocap Data Dictionary:
   - NUMERIC have length up to 10: this appears to mean *characters* e.g. '-24.43' is 6
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

(defn dd-schema [dd-table]
  (let [[header & body] (s/split-lines dd-table)
        ; Reunite lines, including a space at the beginning (see `re-seq` below).
        ; This accommodates multiline definitions in pdf.
        body (apply str (interleave (repeat " ") body))
        [_ table title] (re-find #"(.*)\.DBF (.*)" header)]
    [(keyword table)
     (with-meta
       (into (sorted-map)
         (for [[_ variable definition type length decimals]
               (re-seq #" ([A-Z0-9_]*) (.*?) ([NCDTIML])\s+(\d+)\s+(\d+)" body)]
           [(keyword variable) (cond-> {:name definition
                                        :type (-> ^char (first type) DBFDataType/fromCode types)
                                        :length (Integer/parseInt length)
                                        :decimals (Integer/parseInt decimals)}
                                 (= variable "PRIM_KEY") (assoc :pk true))]))
       {:title title})]))

#_ (def schema
     "Generate schema from vendor's pdf!"
     (->> "JocapXL_data_dictionary.pdf" get-dd dd-tables (map dd-schema)
          (into (sorted-map) (remove (comp nil? first)))))


#_(defn reality-schema
    [dbf-file]
    (let [dbfr (DBFReader. (io/input-stream dbf-file))]
      (into (sorted-map)
            (for [field (map #(.getField dbfr %) (range (.getFieldCount dbfr)))
                  :let [colname (.getName field)
                        coltype (.getType field)]]
              [(keyword colname) {:type (get types coltype)}]))))

#_ (def data-root "")

#_ (def tables (c/ext "dbf" (comp keyword s/upper-case) data-root))

#_ (def extra-columns
     "Which columns appear in the dbf but not the schema?"
     (into (sorted-map)
       (for [[table fields] schema
             :let [file (get tables table)
                   reality (reality-schema file)
                   reality-fields (-> reality keys set)
                   schema-fields (-> fields keys set)
                   missing (clojure.set/difference reality-fields schema-fields)
                   extra (clojure.set/difference schema-fields reality-fields)]
             :when (seq missing)]
         [table (into (sorted-map) (select-keys reality missing))])))

; Gives:
#_ {:A_CPL {:NRATIO {:type 8}},
    :A_MON {:SVR {:type 8}},
    :A_PATEVE {:EV_IN_OU10 {:type 12},
               :EV_IN_OU11 {:type 12},
               :EV_IN_OU12 {:type 12},
               :EV_IN_OU13 {:type 12},
               :EV_IN_OU14 {:type 12},
               :EV_IN_OU15 {:type 12},
               :EV_IN_OU16 {:type 12},
               :EV_IN_OU17 {:type 12},
               :EV_IN_OU18 {:type 12},
               :EV_IN_OU19 {:type 12},
               :EV_IN_OU20 {:type 12},
               :EV_IN_OU21 {:type 12},
               :EV_IN_OU22 {:type 12},
               :EV_IN_OU23 {:type 12},
               :EV_IN_OU24 {:type 12}}}

; Full view of missing and extra:
#_ {:A_ANEST {:missing #{}, :extra #{}},
    :A_BLUT {:missing #{}, :extra #{}},
    :A_CHECK {:missing #{}, :extra #{}},
    :A_CPL {:missing #{:NRATIO}, :extra #{}},
    :A_EREIG {:missing #{}, :extra #{}},
    :A_FREE {:missing #{}, :extra #{}},
    :A_GFM {:missing #{}, :extra #{}},
    :A_MON {:missing #{:SVR}, :extra #{}},
    :A_OFFBGA {:missing #{}, :extra #{}},
    :A_ONBG2 {:missing #{}, :extra #{}},
    :A_ONBGA {:missing #{}, :extra #{}},
    :A_OTHER1 {:missing #{}, :extra #{}},
    :A_OTHER2 {:missing #{}, :extra #{}},
    :A_PAT {:missing #{}, :extra #{}},
    :A_PATEVE {:missing #{:EV_IN_OU12
                          :EV_IN_OU13
                          :EV_IN_OU23
                          :EV_IN_OU19
                          :EV_IN_OU14
                          :EV_IN_OU17
                          :EV_IN_OU15
                          :EV_IN_OU20
                          :EV_IN_OU16
                          :EV_IN_OU24
                          :EV_IN_OU10
                          :EV_IN_OU18
                          :EV_IN_OU21
                          :EV_IN_OU11
                          :EV_IN_OU22},
               :extra #{:EV_IN_OUT18
                        :EV_IN_OUT22
                        :EV_IN_OUT14
                        :EV_IN_OUT13
                        :EV_IN_OUT17
                        :EV_IN_OUT20
                        :EV_IN_OUT24
                        :EV_IN_OUT10
                        :EV_IN_OUT23
                        :EV_IN_OUT15
                        :EV_IN_OUT16
                        :EV_IN_OUT11
                        :EV_IN_OUT19
                        :EV_IN_OUT21
                        :EV_IN_OUT12}},
    :A_PERF {:missing #{}, :extra #{}},
    :PATGG {:missing #{}, :extra #{}}}

#_ (def extra-tables
     "Which target tables are missing from schema? Infer their schema."
     (into (sorted-map)
       (for [missing (clojure.set/difference j/target-tables (-> schema keys set))
             :let [file (get tables missing)]]
         [missing (reality-schema file)])))

; Gives:
#_ {:TRACK {:CDBF {:type 12},
            :CFLD {:type 12},
            :CKEY {:type 12},
            :COLDVAL {:type -1},
            :CPATNR {:type 12},
            :CTYPE {:type 12},
            :CUSER {:type 12},
            :TTIME {:type 93}}}

#_ (c/safe-spit-edn "resources/jocap-schema.edn"
                    (merge-with merge schema extra-columns extra-tables))

(def schema j/schema)

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

#_ (def units ; TODO might be useful later during parsing; might not reflect reality so don't change schema
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