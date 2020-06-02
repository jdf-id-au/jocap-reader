(ns user
  (:require [jocap-reader.core :as j]
            [comfort.core :as c]))

#_(def a_pat (->> "path" (c/ext "dbf" namer) :A_PAT j/open-dbf j/dbf-row-seq doall))
