(ns adgoji.cascalog.graph
  (:require
   [clojure.stacktrace]
   [clojure.set]
   [plumbing.core :as gc]
   [plumbing.graph :as graph]
   [plumbing.fnk.pfnk :as pfnk]
   [cascalog.api :as cas]
   [adgoji.cascalog.checkpoint  :as checkpoint]))

(defn fnk-type [fnk]
  (::fnk-type (meta fnk)))

(defn mk-typed-fnk [t f]
  (vary-meta f assoc ::fnk-type t))

(defn fn->typed-fnk [t f io-schemata]
  (mk-typed-fnk t (pfnk/fn->fnk f io-schemata)))

(def fn->query-fnk (partial fn->typed-fnk :query))

;; Duplicate graphs fnk for convenience
(defmacro fnk [& args]
  `(gc/fnk ~@args))

(defmacro query-fnk [& args]
  `(mk-typed-fnk :query (gc/fnk ~@args)))

;; TODO clean up this macro as well, use vary-meta
(defmacro tmp-dir-fnk [& args]
  `(vary-meta (mk-typed-fnk :tmp-dir (gc/fnk ~@args))
             update-in [::pfnk/io-schemata 0] dissoc :tmp-dir))

;; DEPRECATED replaced by transact
(defmacro final-fnk [& args]
  `(mk-typed-fnk :final (gc/fnk ~@args)))

(defn fnk-input-keys [pfnk-val]
  (keys (pfnk/input-schema pfnk-val)))

(def fnk-deps fnk-input-keys)

(defn dependency-graph [g]
  (reduce (fn [acc [k v]]
            (reduce (fn [acc0 dep]
                      (update-in acc0 [dep] (fnil conj []) k)) acc
                      (fnk-input-keys v))) {} g))

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
                                                               (cas/?- output-tap input-tap))
                                                             [{k true v true} true])))
                        ;; Update old function keep old-schemata plus the output tap schemata
                        ;; -
                        ;; (?- ~(symbol (name v)) (apply prev-fn args)
                        (update-in g [k] (fn [prev-fn]
                                           (let [prev-input-args (fnk-input-keys prev-fn)]
                                             (pfnk/fn->fnk (fn [{output-tap v :as args}]
                                                             (cas/?- output-tap (prev-fn (select-keys args prev-input-args))))
                                                           (update-in (pfnk/io-schemata prev-fn) [0] assoc v true)))))))
                    graph (filter (comp graph key) output-mapping))))

(defn transact
  "Create a new graph in which the `nodes-after have the intermediate nodes as dependency
   This can be used to create a sort of transaction, e.g. cleaning up Pail snapshots"
  [graph nodes-before nodes-after]
  (let [all-nodes (keys graph)
        io-schemata-all-nodes (interleave all-nodes (repeat true))
        nodes-after (into {} (map (fn [[k f]] [k
                                              (vary-meta f update-in
                                                         [:plumbing.fnk.pfnk/io-schemata 0]
                                                         (partial apply assoc) io-schemata-all-nodes)]) nodes-after))]
    (merge nodes-before graph nodes-after)))

(defn steps-graph [graph]
  (set (keys graph)))

(defn deps-serial [graph]
  (let [steps (steps-graph graph)]
    (first
     (reduce (fn [[acc previous-step] [k f]]
               [(assoc acc k [previous-step]) k])
             [{} nil] graph))))

(defn deps-parallel [graph]
  (let [steps (steps-graph graph)]
    (reduce (fn [acc [k f]]
              (assoc acc k (filter steps (fnk-input-keys f))))
            {} graph)))

(defn graph->nodes [workflow graph serial-order?]
  (let [deps-mapping (if serial-order?
                       (deps-serial graph)
                       (deps-parallel graph))]
    (into {}
          (map (fn [[k f]]
                 (let [node-deps (get deps-mapping k)
                       tmp-dir (str (::checkpoint/checkpoint-dir workflow) "/" (name k))
                       ;; TODO can we do without the fnk macros, only needed for this:
                       f-wrapped (condp = (fnk-type f)
                                   :tmp-dir (fn [deps]
                                              (f (assoc deps :tmp-dir tmp-dir)))
                                   :query  (fn [deps]
                                             (let [query (f deps)
                                                   ;; Create seqfile with outfields of query to support convenience functions
                                                   ;; such as (select-fields my-tap ["?a"])
                                                   intermediate-seqfile (cas/hfs-seqfile tmp-dir
                                                                                         :outfields (cas/get-out-fields query))]
                                               ;; Run query and return seqfile
                                               (cas/?- (name k) intermediate-seqfile query)
                                               intermediate-seqfile))
                                   f)]
                   [k (checkpoint/mk-node workflow {:name k :fn f-wrapped :tmp-dir tmp-dir :deps node-deps})]))
               graph))))

(defn workflow-compile [graph & [options]]
  (let [graph (graph/->graph graph)]
    (pfnk/fn->fnk (fn [{:keys [] :as graph-args}]
                    (let [workflow (checkpoint/mk-workflow "/tmp/cascalog-checkpoint-graph" graph-args options)]
                      (checkpoint/exec-workflow! workflow (graph->nodes workflow graph false))
                      @(::checkpoint/graph-atom workflow)))
                  (pfnk/io-schemata graph))))

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
              dep (when-let [node (graph sym)] (fnk-input-keys node))]
        (println "  " (pr-str (name dep)) "->" (pr-str (name sym)) ";")))
    (println "}")))
