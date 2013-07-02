(ns adgoji.cascalog.graph
  ;; from cascalog checkpoint
  (:use [cascalog.api :only [with-job-conf]])
  (:require [hadoop-util.core :as h]
            [cascalog.conf :as conf]
            [jackknife.core :as u]
            [jackknife.seq :as seq])
  (:import [java.util Collection]
           [org.apache.log4j Logger]
           [java.util.concurrent Semaphore]
           [org.apache.hadoop.fs FileSystem Path]
           [org.apache.hadoop.conf Configuration]
           java.io.FileOutputStream
           java.io.ObjectOutputStream
           java.io.ObjectInputStream java.io.FileInputStream)

  (:require
   [clojure.stacktrace]
   [plumbing.core :as gc]
            [plumbing.graph :as graph]
            [plumbing.fnk.pfnk :as pfnk]
            [cascalog.api :refer [?- hfs-seqfile]]
            [cascalog.checkpoint  :as checkpoint]))

(defn dependency-graph [g]
  (reduce (fn [acc [k v]]
            (reduce (fn [acc0 dep]
                      (update-in acc0 [dep] (fnil conj []) k)) acc
                      (keys (pfnk/input-schema v)))) {} g))

(defn steps-dependent [g k]
  (k (dependency-graph g)))


(defn graphify [graph-like]
  (graph/->graph graph-like))

(defn select-nodes
  "Makes a new graph (see Prismatic's Graph lib) based on the given output mapping. The new graph
   can be a subset of the original graph when the original graph has nodes that are unneeded
   to calculate the output

  `(mk-graph-fnk {} {:alpha :alpha-tap})"
  [graph output-mapping]
  {:pre [(clojure.set/superset? (set (keys graph)) (set (keys output-mapping)))]}
  (graphify (reduce (fn [g [k v]]
                      (if (seq (steps-dependent g k))
                        (let [output-node (-> k name (str "-sink-step") keyword)]
                          (assoc g output-node (pfnk/fn->fnk (fn [{input-tap k output-tap v}]
                                                               (?- output-tap input-tap))
                                                             [{k true v true} true])))
                        ;; Update old function keep old-schemata plus the output tap schemata
                        ;; -
                        ;; (?- ~(symbol (name v)) (apply prev-fn args)
                        (update-in g [k] (fn [prev-fn]
                                           (let [prev-input-args (keys (pfnk/input-schema prev-fn))]
                                             (pfnk/fn->fnk (fn [{output-tap v :as args}]
                                                             (?- output-tap (prev-fn (select-keys args prev-input-args))))
                                                           (update-in (pfnk/io-schemata prev-fn) [0] assoc v true)))))))
                    graph (filter (comp graph key) output-mapping))))

(defn fnk-type [fnk]
  (::fnk-type (meta fnk)))

(defn fnk-deps [fnk]
  (keys (pfnk/input-schema fnk)))

(defmacro tmp-dir-fnk [& args]
  (let [f (eval `(gc/fnk ~@args))
        schemata (update-in (pfnk/io-schemata f) [0] dissoc :tmp-dir)
        f (pfnk/fn->fnk f schemata)
        m (assoc (meta f)  ::fnk-type :tmp-dir)]
    `(with-meta ~f ~m)))

(defn mk-query-fnk [fn]
  (vary-meta fn assoc ::fnk-type :query))

(defn fn->query-fnk [fn io-schemata]
  (mk-query-fnk (pfnk/fn->fnk fn io-schemata)))

;; Duplicate graphs fnk for convenience
(defmacro fnk [& args]
  `(gc/fnk ~@args))

(defmacro query-fnk [& args]
  `(mk-query-fnk (gc/fnk ~@args)))

(defmacro final-fnk [& args]
  `(vary-meta (gc/fnk ~@args) assoc ::fnk-type :final))


;; IDEA for wrapping all nodes in a graph
(defn transact [graph before-key before-fnk after-key after-fnk]
  (let [all-nodes (keys graph)
        adapted-after-fnk (vary-meta after-fnk update-in
                                     [:plumbing.fnk.pfnk/io-schemata 0]
                                     (partial apply assoc) (interleave all-nodes (repeat true)))]
  (assoc graph
      before-key before-fnk
      after-key adapted-after-fnk)))

(defn serializable? [obj]
  (instance? java.io.Serializable obj))

(defn serialize-state [state-file state]
  (let [serializable-state (into {} (filter (comp serializable? val) state))]
    (with-open [file-out (FileOutputStream. state-file)
                object-out (ObjectOutputStream. file-out)]
      (.writeObject object-out serializable-state))))

(defn deserialize-state [state-file]
  (with-open [file-in (FileInputStream. state-file)
              object-in (ObjectInputStream. file-in)]
    (.readObject object-in)))

(defstruct WorkflowNode ::tmp-dirs ::fn ::deps ::error ::value)
(defstruct Workflow ::fs ::checkpoint-dir ::graph-atom ::previous-steps ::sem ::log)

(defn mk-workflow [checkpoint-dir initial-graph]
  (let [fs (h/filesystem)
        log (Logger/getLogger "checkpointed-workflow")
        sem (Semaphore. 0)
        state-file (clojure.java.io/file (str checkpoint-dir "/state.bin"))
        previous-steps (when (.exists state-file) (deserialize-state state-file))]
    (h/mkdirs fs checkpoint-dir)
    (struct Workflow fs checkpoint-dir (atom initial-graph) previous-steps sem log)))

(defn- mk-runner
  [node workflow]
  (let [log (::log workflow)
        graph-atom (::graph-atom workflow)
        previous-steps (::previous-steps workflow)
        sem (::sem workflow)
        node-name (::name node)
        config conf/*JOB-CONF*]
    (Thread.
     (fn []
       (with-job-conf config
         (try
           ;; Check previous value, doesn't take into account changes graph
           ;; removing checkpoint dir is the only option for these case
           ;; TODO smarter graph checking
           (let [value (if-let [previous-value (get previous-steps node-name)]
                         (do (.info log (str "Skipping " node-name "..."))
                             previous-value)
                         (do (.info log (str "Calculating " node-name "..."))
                             ((::fn node) @graph-atom)))]
             (swap! graph-atom assoc node-name value)
             (reset! (::value node) value))
           (reset! (::status node) :successful)
           (catch Throwable t
             (.error log (str "Component failed " node-name) t)
             (reset! (::error node) t)
             (reset! (::status node) :failed))
           (finally (.release sem))))))))

(defn- fail-workflow!
  [workflow nodes-map]
  (let [log (::log workflow)
        nodes (vals nodes-map)
        failed-nodes (filter (comp deref ::error) nodes)
        succesful-nodes (filter (comp (partial = :successful) deref ::status) nodes)
        running-nodes (filter (comp (partial = :running) deref ::status) nodes)
        threads (map ::runner-thread running-nodes)]
    (.info log "Workflow failed - interrupting components")
    (doseq [t threads] (.interrupt t))
    (.info log "Waiting for running components to finish")
    (doseq [t threads] (.join t))
    (.info log "Serializing succesful nodes")
    (serialize-state (str (::checkpoint-dir workflow) "/state.bin")
                     (into {} (map (fn [n] [(::name n) @(::value n)]) succesful-nodes)))
    ;; TODO we would like to print the Exceptions that help us fix bugs, but it is unclear
    ;;      how to reach those Exceptions (e.g. TupleExceptions)
    (u/throw-runtime (str "Workflow failed during " (clojure.string/join ", " (map ::name failed-nodes))))))

(defn graph->nodes [workflow graph]
  (let [previous-steps (::previous-steps workflow)
        steps (set (keys graph))]
    (into {}
          (map (fn [[k f]]
                 (let [node-deps (filter steps (keys (pfnk/input-schema f)))
                       tmp-dir (str (::checkpoint-dir workflow) "/" (name k))

                       ;; TODO can we do without the fnk macros, only needed for this:
                       f-wrapped (condp = (fnk-type f)
                                   :tmp-dir (fn [deps]
                                               (f (assoc deps :tmp-dir tmp-dir)))
                                   :query  (fn [deps]
                                             (let [query (f deps)
                                                   ;; Create seqfile with outfields of query to support convenience functions
                                                   ;; such as (select-fields my-tap ["?a"])
                                                   intermediate-seqfile (apply hfs-seqfile tmp-dir
                                                                               (apply concat (select-keys query [:outfields])))]
                                               ;; Run query and return seqfile
                                               (?- (name k) intermediate-seqfile query)
                                               intermediate-seqfile))
                                   f)
                       node
                       (struct-map WorkflowNode
                         ::name k
                         ::fn f-wrapped
                         ::status (atom :unstarted)
                         ::error (atom nil)
                         ::value (atom nil)
                         ::tmp-dir tmp-dir
                         ::deps node-deps)]
                   [k (assoc node ::runner-thread (mk-runner node workflow))]))
               graph))))

(defn exec-workflow! [workflow nodes]
  ;; run checkpoint as graph
  (let [log (::log workflow)]
    (loop []
      ;; Start nodes t
      (doseq [[name node] nodes
              :when (and (= :unstarted @(::status node))
                         (every? (fn [[_ n]] (= :successful @(::status n)))
                                 (select-keys nodes (::deps node))))]
        (reset! (::status node) :running)
        (.start (::runner-thread node)))
      ;; Wait for nodes to finish
      (.acquire (::sem workflow))
      ;; Check for failures
      (let [statuses (set (map (comp deref ::status val) nodes))]
        (cond (contains? statuses :failed) (fail-workflow! workflow nodes)
              (some #{:running :unstarted} statuses) (recur)
              :else (.info log "Workflow completed successfully"))))
    ;; TODO make deleting checkpoint dir optional
    #_(h/delete (::fs workflow) (::checkpoint-dir workflow) true)))

(defn workflow-compile [graph]
  (let [graph (graph/->graph graph)]
    (pfnk/fn->fnk (fn [{:keys [] :as graph-args}]
                    (let [workflow (mk-workflow "/tmp/cascalog-checkpoint-graph" graph-args)]
                      (exec-workflow! workflow (graph->nodes workflow graph)))) (pfnk/io-schemata graph))))

;; Adopted from https://github.com/stuartsierra/flow/blob/master/src/com/stuartsierra/flow.clj#L138
(defn dot-compile
  "Prints a representation of the workflow to standard output,
  suitable for input to the Graphviz 'dot' program. Options are
  key-value pairs from:

    :graph-name   string/symbol/keyword naming the graph. Must not be
                  a Graphviz reserved word such as \"graph\"."
  [graph & [options]]
  (let [{:keys [graph-name]
         :or {graph-name "graph"}} options
         graph (graph/->graph graph)]
    (println "digraph" (pr-str (name graph-name)) "{")
    (when (map? graph)
      (doseq [sym (keys graph)
              dep (when-let [node (graph sym)] (keys (pfnk/input-schema node)))]
        (println "  " (pr-str (name dep)) "->" (pr-str (name sym)) ";")))
    (println "}")))
