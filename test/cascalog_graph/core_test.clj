(ns cascalog-graph.core-test
  (:use clojure.test
        adgoji.cascalog.graph
        cascalog.api))

(require '[cascalog.api :as cascalog]
         '[adgoji.cascalog.graph :as g])

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

(deftest flow
  (testing "Workflow output"
    (is (= (flow->workflow complete-flow)
           '(gamma-step ([:deps nil :tmp-dirs [gamma-dir]]
             ":gamma: " (cascalog.api/?- (cascalog.api/hfs-seqfile gamma-dir) (cascalog-graph.core-test/gamma {:beta beta, :alpha alpha})))
             delta-step ([:deps [gamma-step] :tmp-dirs [delta-dir]]
             ":delta: " (cascalog.api/?- (cascalog.api/hfs-seqfile delta-dir) (cascalog-graph.core-test/delta {:gamma (cascalog.api/hfs-seqfile gamma-dir), :alpha alpha})))
             epsilon-step ([:deps [delta-step gamma-step] :tmp-dirs [epsilon-dir]]
             ":epsilon: " (cascalog.api/?- (cascalog.api/hfs-seqfile epsilon-dir) (cascalog-graph.core-test/epsilon {:gamma (cascalog.api/hfs-seqfile gamma-dir), :delta (cascalog.api/hfs-seqfile delta-dir)})))
             result-step ([:deps [epsilon-step delta-step gamma-step]]
             ":result: " (cascalog-graph.core-test/result {:gamma (cascalog.api/hfs-seqfile gamma-dir), :delta (cascalog.api/hfs-seqfile delta-dir), :output-tap output-tap, :epsilon (cascalog.api/hfs-seqfile epsilon-dir)})))
           ))))
