(ns example
  (:require 
    [cascalog.api :as cascalog :refer :all]
    [adgoji.cascalog.graph :as g]))

(def result (g/flow-fn [gamma delta epsilon output-tap]
  (?<- output-tap 
    [?result]
    (gamma ?idx ?gamma)
    (delta ?idx ?delta)
    (epsilon ?idx ?epsilon)
    (+ ?gamma ?delta ?epsilon :> ?result))))
        
(def gamma (g/flow-fn [alpha beta]
  (<- [?idx ?gamma]
    (alpha ?idx ?alpha)
    (beta ?idx ?beta)
    (+ ?alpha ?beta :> ?gamma))))

(def delta (g/flow-fn [alpha gamma]
  (<- [?idx ?delta]
    (alpha ?idx ?alpha)
    (gamma ?idx ?gamma)
    (+ ?gamma ?alpha :> ?delta))))

(def epsilon (g/flow-fn [gamma delta]
  (<- [?idx ?epsilon]
    (gamma ?idx ?gamma)
    (delta ?idx ?delta)
    (+ ?gamma ?delta :> ?epsilon))))

(def complete-flow (g/fns-to-flow #'result #'gamma #'delta #'epsilon))

(defn -main [& args]
  ((g/mk-workflow-fn complete-flow) {:output-tap (cascalog/stdout) :alpha [[0 1]] :beta [[0 2]] }))
