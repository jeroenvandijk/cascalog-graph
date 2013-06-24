(ns adgoji.cascalog.graph.test
  (:require [adgoji.cascalog.graph :as g]
            [cascalog.io :as io]
            [cascalog.api :as casc]
            clojure.pprint))

;; TODO Special queries that rely on tmp dirs not supported yet
(defn test-graph [graph inputs & {:keys [log-level] :or {log-level :fatal}}]
  (apply dissoc (first (reduce (fn [[acc step-list] [step-name step]]
      (let [deps (g/fnk-deps step)
            dep-args (select-keys acc deps)
            step* (step dep-args)
            output
              (try
              (io/with-log-level log-level
                (casc/with-job-conf {"io.sort.mb" 10}
                 (if (= (g/fnk-type step) :query)
                  ;; We need to change the list to a vector to prevent "source taps required" errors
                  (vec (first (casc/??- step*)))
                  step*)))
                (catch Exception e
                  (throw (doto (Exception. 
                                 (str "Step " step-name " failed with message: " (.getMessage e)
                                 "\n"
                                 "Called with " dep-args
                                 "\n"
                                 "Step depends on: " deps
                                 "\n"
                                 "Accumulated:\n"
                                 (with-out-str
                                    (clojure.pprint/pprint step-list))
                                 ))
                               (.setStackTrace (.getStackTrace e))))
            ))]
            
            [(assoc acc step-name output) (conj step-list [step-name output])]
            ))
    [inputs []] (g/graphify graph))) (keys inputs)))