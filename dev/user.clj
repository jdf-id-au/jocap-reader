(ns user
  (:require [jocap-reader.core :as j]
            [comfort.core :as c]))

#_ (def lazy (j/extract-cases
               "path"
               [:procnum 20171256]))
#_ (def done (time (doall lazy)))
