(ns adgoji.cascalog.graph
  (:require [plumbing.core :as gc]
            [plumbing.graph :as graph]
            [plumbing.fnk.pfnk :as pfnk]
            [cascalog.api :refer [?- hfs-seqfile]]
            [cascalog.checkpoint :refer [workflow] :as checkpoint]))

(defn- sym-dir [sym]
  (symbol (str (name sym) "-dir")))

(defn- sym-step [keyw]
  (symbol (str (name keyw) "-step")))

(defn- sym-fn [keyw]
  (symbol (name keyw)))

(defn- mk-step-part [name deps tmp-dir body]
  (list
   (sym-step name)
   (list (vec (concat [:deps (cond (= deps :all) deps
                                   (seq deps) (vec deps))]
                      (when tmp-dir [:tmp-dirs [(sym-dir name)]])))
         body)))

(defn- mk-state-fnk-part [name fn-args]
  (list 'save-state name (list (list 'graph# name) fn-args)))

(defn- mk-query-state-fnk-part [step-name fn-args]
  (list 'let (vector 'tmp-seqfile (list `hfs-seqfile (sym-dir step-name)))
        (list `?- (str (name step-name)) 'tmp-seqfile (list (list 'graph# step-name) fn-args))
        (list 'save-state step-name 'tmp-seqfile)))

(defn- mk-tmp-dir-fnk-step [{:keys [name deps fn-args]}]
  (let [tmp-dir (sym-dir name)
        fn-args (assoc fn-args :tmp-dir tmp-dir)]
    (mk-step-part name deps tmp-dir (mk-state-fnk-part name fn-args))))

(defn- mk-fnk-step [{:keys [name deps fn-args]}]
  (mk-step-part name deps nil (mk-state-fnk-part name fn-args)))

(defn- mk-final-fnk-step [{:keys [name deps fn-args]}]
  (mk-step-part name :all nil (mk-state-fnk-part name fn-args)))

(defn- mk-query-fnk-step [{:keys [name deps fn-args]}]
  (let [tmp-dir (sym-dir name)]
      (mk-step-part name deps tmp-dir (mk-query-state-fnk-part name fn-args))))

(defn mk-fn-args [g ks]
  (into {}
        (map (fn [k]
               [k
                (if (g k)
                  (list 'fetch-state k)
                  (symbol (name k)))]) ks)))

(defn dependency-graph [g]
  (reduce (fn [acc [k v]]
            (reduce (fn [acc0 dep]
                      (update-in acc0 [dep] (fnil conj []) k)) acc
                      (keys (pfnk/input-schema v)))) {} g))

(defn steps-dependent [g k]
  (k (dependency-graph g)))

(defn mk-step [g [k v]]
  (let [dep-keys (keys (pfnk/input-schema v))
        fn-args (mk-fn-args g dep-keys)
        deps (map sym-step (clojure.set/intersection (set dep-keys) (set (keys g))))
        args {:name k :deps deps :fn-args fn-args}]
    (condp  = (-> v meta ::fnk-type)
      :query (mk-query-fnk-step args)
      :tmp-dir (mk-tmp-dir-fnk-step args)
      :final (mk-final-fnk-step args)
      (mk-fnk-step args))))

;; Duplicate graphs fnk for convenience
(defmacro fnk [& args]
  `(gc/fnk ~@args))

(defn graphify [graph-like]
  (graph/->graph graph-like))

(defn mk-workflow
  ([tmp-dir graph-like] (mk-workflow tmp-dir graph-like {} {}))
  ([tmp-dir graph-like input-mapping output-mapping]
     (let [graph-like (reduce (fn [g [k v]]
                           (if (seq (steps-dependent g k))
                             (assoc g v (pfnk/fn->fnk (fn [{input-tap k output-tap v}]
                                                        (?- output-tap input-tap))
                                                      [{k true} true]))
                             ;; Update old function keep old-schemata plus the output tap schemata
                             ;; -
                             ;; (?- ~(symbol (name v)) (apply prev-fn args)
                             (update-in g [k] (fn [prev-fn]
                                                (pfnk/fn->fnk (fn [{input-tap k output-tap v :as args}]
                                                                (?- output-tap (apply prev-fn (dissoc args v))))
                                                              (update-in (pfnk/io-schemata prev-fn) [0] assoc v true))))))
                         graph-like (filter (comp graph-like key) output-mapping))
           graph (graphify graph-like)
           input-keywords (set (keys (pfnk/input-schema graph)))
           input-mapping (select-keys input-mapping input-keywords)
           input-keys (mapv (comp symbol name)
                            (concat (clojure.set/difference input-keywords (keys input-mapping))
                                    (vals input-mapping)))]
       (list 'let (vector 'graph# graph)
             (list `fnk input-keys
                   (list
                    'let (vec (concat
                               (map (comp symbol name) (flatten (seq input-mapping)))
                               '[state (atom {})
                                 save-state (fn [k v] (swap! state assoc k v))
                                 fetch-state (fn [k] (@state k))]))
                    (list 'do (concat (list `checkpoint/workflow [tmp-dir])
                                      (mapcat (partial mk-step graph) graph))
                          'state)))))))

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

(defmacro query-fnk [& args]
  (let [f (eval `(gc/fnk ~@args))
        m (assoc (meta f) ::fnk-type :query)]
    `(with-meta ~f ~m)))

(defmacro final-fnk [& args]
  (let [f (eval `(gc/fnk ~@args))
        m (assoc (meta f) ::fnk-type :final)]
    `(with-meta ~f ~m)))

(defn workflow-compile
  ([graph] (workflow-compile "/tmp/cascalog-checkpoint" graph))
  ([tmp-dir graph]
     (eval (mk-workflow tmp-dir graph))))



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
