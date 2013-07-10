(ns test-queries
  (:use cascalog.api adgoji.cascalog.graph adgoji.cascalog.cli)
  (:require [adgoji.cascalog.cli.tap :as tap]
            [adgoji.cascalog.graph :as graph]))

(defmethod tap/mk-tap "inline" [_]
  {:tap-fn (fn [{:keys [path params] :as args}]
             (read-string path))
   :validation (fn [{:keys [path params] :as args}]
                 ;; Correct when we can read-string without error
                 (try (read-string path) nil (catch Exception e (.getMessage e))))
   :allowed-types [:source]})

(def flow { :result (query-fnk [gamma delta epsilon]
                               (<-
                                [?result]
                                (gamma ?idx ?gamma)
                                (delta ?idx ?delta)
                                (epsilon ?idx ?epsilon)
                                (+ ?gamma ?delta ?epsilon :> ?result)))
            :gamma (query-fnk [alpha-tap beta-tap]
                              (<- [?idx ?gamma]
                                  (alpha-tap ?idx ?alpha-tap)
                                  (beta-tap ?idx ?beta-tap)
                                  (+ ?alpha-tap ?beta-tap :> ?gamma)))
           :delta (query-fnk [alpha-tap gamma]
                             (<- [?idx ?delta]
                                 (alpha-tap ?idx ?alpha-tap)
                                 (gamma ?idx ?gamma)
                                 (+ ?gamma ?alpha-tap :> ?delta)))
            :epsilon (query-fnk [gamma delta]
                                (<- [?idx ?epsilon]
                                    (gamma ?idx ?gamma)
                                    (delta ?idx ?delta)
                                    (+ ?gamma ?delta :> ?epsilon)))})
;; lein run -m test-queries/foo --beta-tap "inline:[[0 1]]" --alpha-tap "inline:[[0 1]]" --gamma-output-tap stdout 
(defjob foo (graph/select-nodes flow {:gamma :gamma-output-tap}))
