(ns dev.schema
  (:require [clojure.string :as s]
            [clojure.java.io :as io]
            [jocap-reader.core :as j]
            [comfort.core :as c])
  (:import (org.apache.pdfbox.pdmodel PDDocument)
           (org.apache.pdfbox.text PDFTextStripper)
           (com.linuxense.javadbf DBFDataType DBFReader)))

(defn get-dd [filename]
  (with-open [pdf (->> filename io/resource io/file PDDocument/load)]
    (.getText (PDFTextStripper.) pdf)))

(defn dd-tables [dd]
  (s/split dd #"Table "))

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
                                        :type (-> ^char (first type) DBFDataType/fromCode j/types)
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
              [(keyword colname) {:type (get j/types coltype)}]))))

#_ (def data-root "")

#_ (def tables (c/ext "dbf" j/namer data-root))

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