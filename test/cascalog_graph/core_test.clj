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


(let [g {:a (tmp-dir-fnk [b] b)}]
 (fact "`tmp-dir-fnk is correctly translated to a workflow step"
   (mk-step g (first g)) => '(a-step ([:deps nil :tmp-dirs [a-dir]]
                                        (save-state :a ((graph# :a) {:b b :tmp-dir a-dir}))))
   ))

(let [g {:a (fnk [b] b)}]
 (fact "`fnk is correctly translated to a workflow step"
   (mk-step g (first g)) => '(a-step ([:deps nil]
                                        (save-state :a ((graph# :a) {:b b}))))
   ))

(let [g {:a (query-fnk [b] b)}]
 (fact "`query-fnk is correctly translated to a workflow step"
   (mk-step g (first g)) => '(a-step ([:deps nil :tmp-dirs [a-dir]]
                                        (let [tmp-seqfile (cascalog.api/hfs-seqfile a-dir)]
                                          (cascalog.api/?- "a" tmp-seqfile ((graph# :a) {:b b}))
                                          (save-state :a tmp-seqfile))))
   ))

(let [g {:a (final-fnk [b] b)}]
 (fact "`final-fnk is correctly translated to a workflow step"
   (mk-step g (first g)) => '(a-step ([:deps :all]
                                        (save-state :a ((graph# :a) {:b b }))))
   ))

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
 (fact "correct workflow renamed workflow"
   (last (mk-workflow "tmp/foo" graph {:alpha :alpha-renamed} {})) =>
   '(adgoji.cascalog.graph/fnk [tmp-state beta alpha-renamed]
         (let [alpha alpha-renamed
               state (atom {})
               save-state (fn [k v] (swap! state assoc k v))
               fetch-state (fn [k] ((clojure.core/deref state) k))]
           (do
             (cascalog.checkpoint/workflow ["tmp/foo"]
                                           gamma-step ([:deps nil :tmp-dirs [gamma-dir]]
                                                         (let [tmp-seqfile (cascalog.api/hfs-seqfile gamma-dir)]
                                                           (cascalog.api/?- "gamma" tmp-seqfile ((graph# :gamma) {:alpha alpha, :beta beta}))
                                                           (save-state :gamma tmp-seqfile)))
                                           delta-step ([:deps [gamma-step] :tmp-dirs [delta-dir]]
                                                         (let  [tmp-seqfile (cascalog.api/hfs-seqfile delta-dir)]
                                                           (cascalog.api/?- "delta" tmp-seqfile ((graph# :delta) {:alpha alpha, :gamma (fetch-state :gamma)}))
                                                           (save-state :delta tmp-seqfile)))
                                           epsilon-step ([:deps [delta-step gamma-step] :tmp-dirs [epsilon-dir]]
                                                           (let [tmp-seqfile (cascalog.api/hfs-seqfile epsilon-dir)]
                                                             (cascalog.api/?- "epsilon" tmp-seqfile ((graph# :epsilon) {:delta (fetch-state :delta), :gamma (fetch-state :gamma)}))
                                                             (save-state :epsilon tmp-seqfile)))
                                           result-step ([:deps [epsilon-step delta-step gamma-step] :tmp-dirs [result-dir]]
                                                          (save-state :result ((graph# :result) {:delta (fetch-state :delta), :epsilon (fetch-state :epsilon), :gamma (fetch-state :gamma), :tmp-dir result-dir})))
                                           final-step-step ([:deps :all]
                                                              (save-state :final-step ((graph# :final-step) {:tmp-state tmp-state})))
                                           read-result-step ([:deps [result-step]]
                                                               (save-state :read-result ((graph# :read-result) {:result (fetch-state :result) :tmp-state tmp-state}))))
             state))))

 (fact "correct workflow renamed workflow for outputs"
       (last (mk-workflow "tmp/foo" graph {:alpha :alpha-renamed} {:epsilon :epsilon-tap})) =>
       '(adgoji.cascalog.graph/fnk [tmp-state beta alpha-renamed]
                                   (let [alpha alpha-renamed
                                         state (atom {})
                                         save-state (fn [k v] (swap! state assoc k v))
                                         fetch-state (fn [k] ((clojure.core/deref state) k))]
                                     (do (cascalog.checkpoint/workflow ["tmp/foo"]
                                                                       gamma-step ([:deps nil :tmp-dirs [gamma-dir]]
                                                                                     (let [tmp-seqfile (cascalog.api/hfs-seqfile gamma-dir)]
                                                                                       (cascalog.api/?- "gamma" tmp-seqfile
                                                                                                        ((graph# :gamma) {:alpha alpha, :beta beta})) (save-state :gamma tmp-seqfile)))
                                                                       delta-step ([:deps [gamma-step] :tmp-dirs [delta-dir]]
                                                                                     (let [tmp-seqfile (cascalog.api/hfs-seqfile delta-dir)]
                                                                                       (cascalog.api/?- "delta" tmp-seqfile ((graph# :delta) {:alpha alpha, :gamma (fetch-state :gamma)}))
                                                                                       (save-state :delta tmp-seqfile)))
                                                                       epsilon-step ([:deps [delta-step gamma-step] :tmp-dirs [epsilon-dir]]
                                                                                       (let [tmp-seqfile (cascalog.api/hfs-seqfile epsilon-dir)]
                                                                                         (cascalog.api/?- "epsilon" tmp-seqfile ((graph# :epsilon) {:delta (fetch-state :delta), :gamma (fetch-state :gamma)}))
                                                                                         (save-state :epsilon tmp-seqfile)))
                                                                       epsilon-tap-step ([:deps [epsilon-step]]
                                                                                           (save-state :epsilon-tap ((graph# :epsilon-tap) {:epsilon (fetch-state :epsilon)})))
                                                                       result-step ([:deps [epsilon-step delta-step gamma-step] :tmp-dirs [result-dir]]
                                                                                      (save-state :result ((graph# :result) {:delta (fetch-state :delta), :epsilon (fetch-state :epsilon), :gamma (fetch-state :gamma), :tmp-dir result-dir})))
                                                                       final-step-step ([:deps :all]
                                                                                          (save-state :final-step ((graph# :final-step) {:tmp-state tmp-state})))
                                                                       read-result-step ([:deps [result-step]]
                                                                                           (save-state :read-result ((graph# :read-result) {:result (fetch-state :result), :tmp-state tmp-state})))) state))))

 (fact "correct workflow"
   (last (mk-workflow "tmp/foo" graph)) =>
   '(adgoji.cascalog.graph/fnk [ alpha tmp-state beta]
         (let [state (atom {})
               save-state (fn [k v] (swap! state assoc k v))
               fetch-state (fn [k] ((clojure.core/deref state) k))]
           (do
             (cascalog.checkpoint/workflow ["tmp/foo"]
                                           gamma-step ([:deps nil :tmp-dirs [gamma-dir]]
                                                         (let [tmp-seqfile (cascalog.api/hfs-seqfile gamma-dir)]
                                                           (cascalog.api/?- "gamma" tmp-seqfile ((graph# :gamma) {:alpha alpha, :beta beta}))
                                                           (save-state :gamma tmp-seqfile)))
                                           delta-step ([:deps [gamma-step] :tmp-dirs [delta-dir]]
                                                         (let  [tmp-seqfile (cascalog.api/hfs-seqfile delta-dir)]
                                                           (cascalog.api/?- "delta" tmp-seqfile ((graph# :delta) {:alpha alpha, :gamma (fetch-state :gamma)}))
                                                           (save-state :delta tmp-seqfile)))
                                           epsilon-step ([:deps [delta-step gamma-step] :tmp-dirs [epsilon-dir]]
                                                           (let [tmp-seqfile (cascalog.api/hfs-seqfile epsilon-dir)]
                                                             (cascalog.api/?- "epsilon" tmp-seqfile ((graph# :epsilon) {:delta (fetch-state :delta), :gamma (fetch-state :gamma)}))
                                                             (save-state :epsilon tmp-seqfile)))
                                           result-step ([:deps [epsilon-step delta-step gamma-step] :tmp-dirs [result-dir]]
                                                          (save-state :result ((graph# :result) {:delta (fetch-state :delta), :epsilon (fetch-state :epsilon), :gamma (fetch-state :gamma), :tmp-dir result-dir})))
                                           final-step-step ([:deps :all]
                                                              (save-state :final-step ((graph# :final-step) {:tmp-state tmp-state})))
                                           read-result-step ([:deps [result-step]]
                                                               (save-state :read-result ((graph# :read-result) {:result (fetch-state :result) :tmp-state tmp-state}))))
             state))))

 (fact "running a workflow works as expected"
   (-> ((workflow-compile graph) { :tmp-state (atom {}) :alpha [[0 1]] :beta [[0 2]]} )
       deref :read-result) => 14))

(require ' [plumbing.graph :as pris-graph])

(let [graph (pris-graph/->graph {:a (fnk [b] b) :b (fnk [c] c)})]
  (fact "`steps-dependent gives list of steps that are depening on a given step"
        (steps-dependent graph :unknown) => nil
        (steps-dependent graph :a) => nil
        (steps-dependent graph :b) => [:a]
        (steps-dependent graph :c) => [:b]))
