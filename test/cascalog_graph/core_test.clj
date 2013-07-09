(ns cascalog-graph.core-test
  (:require [adgoji.cascalog.graph :as g :refer :all]
            [cascalog.api :as cascalog :refer :all]
            [midje.sweet :refer :all]))

(facts "Fnks macros add the correct metadata"
 (meta (final-fnk [b] b)) => {:adgoji.cascalog.graph/fnk-type :final
                              :plumbing.fnk.pfnk/io-schemata [{:b true} true]}
 (meta (query-fnk [b] b)) => {:adgoji.cascalog.graph/fnk-type :query
                              :plumbing.fnk.pfnk/io-schemata [{:b true} true]}
 (meta (tmp-dir-fnk [b] b)) => {:adgoji.cascalog.graph/fnk-type :tmp-dir
                                :plumbing.fnk.pfnk/io-schemata [{:b true} true]})

;; Example adopted from https://github.com/stuartsierra/flow with some added steps
;; to test implementation specifics
(let [result (tmp-dir-fnk [gamma delta epsilon tmp-dir]
                         (?<- (hfs-seqfile tmp-dir)
                              [?result]
                              (gamma ?idx ?gamma)
                              (delta ?idx ?delta)
                              (epsilon ?idx ?epsilon)
                              (+ ?gamma ?delta ?epsilon :> ?result))
                         ;; Return tmp-dir seqfile so we can use it again (just for testing, normally you would use query-fnk)
                         (hfs-seqfile tmp-dir)
                         )


     gamma (query-fnk [alpha beta]
                      (<- [?idx ?gamma]
                          (alpha ?idx ?alpha)
                          (beta ?idx ?beta)
                          (+ ?alpha ?beta :> ?gamma)))

     delta (query-fnk [alpha gamma]
                      (<- [?idx ?delta]
                          (alpha ?idx ?alpha)
                          (gamma ?idx ?gamma)
                          (+ ?gamma ?alpha :> ?delta)))

     epsilon (query-fnk [gamma delta]
                        (<- [?idx ?epsilon]
                            (gamma ?idx ?gamma)
                            (delta ?idx ?delta)
                            (+ ?gamma ?delta :> ?epsilon)))
     graph { :result result
            :gamma gamma
            :delta delta
            :epsilon epsilon
            ;; Test the tmp-dir-fnk by reading the return value of :result and save it to an atom
            ;; the final-step is not dependent on read-result but should be able to use it return value because it is using `final-fnk
            :read-result (fnk [result tmp-state] (reset! tmp-state (first (ffirst (??- result)))))
            :final-step (final-fnk [tmp-state] @tmp-state)
            }]

 (fact "running a workflow works as expected"
   (-> ((workflow-compile graph) { :tmp-state (atom {}) :alpha [[0 1]] :beta [[0 2]]} )
       :read-result) => 14))

(require ' [plumbing.graph :as pris-graph])

(let [graph (pris-graph/->graph {:a (fnk [b] b) :b (fnk [c] c)})]
  (fact "`steps-dependent gives list of steps that are depening on a given step"
        (steps-dependent graph :unknown) => nil
        (steps-dependent graph :a) => nil
        (steps-dependent graph :b) => [:a]
        (steps-dependent graph :c) => [:b]))
