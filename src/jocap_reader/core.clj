(ns jocap-reader.core
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]))

(def target-tables
  "Jocap tables which were linked into PDUTransfer.mdb via ODBC."
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

(def schema (-> "jocap-schema.edn" io/resource slurp edn/read-string))