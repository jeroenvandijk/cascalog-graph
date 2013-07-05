(ns cascalog_graph.test_test
  (require [adgoji.cascalog.graph :as g])
  (:use adgoji.cascalog.graph.test
        midje.sweet
        midje.cascalog.impl
        cascalog.api
        ))

(def graph { :foo (g/query-fnk [input-tap] (<- [?a] (input-tap ?a _)) )})

(let [output (test-graph graph { :input-tap [["Telegraaf" 1]]})] 
  (fact "successful tests will pass"
    (output :foo) => [["Telegraaf"]]))
    
