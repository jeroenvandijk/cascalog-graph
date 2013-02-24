(ns adgoji.cascalog.graph
  (:require [com.stuartsierra.flow :as f]
            [clojure.tools.namespace.dependency :as dep]
            [cascalog.api :refer [?- hfs-seqfile]]
            [cascalog.checkpoint :refer [workflow] :as checkpoint]))

(defn- sym-dir [sym]
  (symbol (str (name sym) "-dir")))

(defn- sym-step [keyw]
  (symbol (str (name keyw) "-step")))

(defn- sym-fn [keyw]
  (symbol (name keyw)))

(defn- mk-intermediate-command [tmp-dir-sym fn-sym input-tap-sym]
  (list `?- (list `hfs-seqfile tmp-dir-sym) (list fn-sym input-tap-sym)))

(defn- mk-intermediate-step [{:keys [name deps fn-sym fn-args fn-doc]}]
  (let [tmp-dir (sym-dir name)
        step-sym (sym-step name)
        doc-str (str name ": " fn-doc)]
    (list
      step-sym (list [:deps (if (empty? deps) nil (vec deps)) :tmp-dirs [tmp-dir]]
        doc-str
        (mk-intermediate-command tmp-dir  (sym-fn fn-sym)  fn-args)))))

(defn- mk-endpoint-step [{:keys [name deps fn-sym fn-args fn-doc]}]
  (let [step-sym (sym-step name)
        doc-str (str name ": " fn-doc)]
    (list
      step-sym (list [:deps (if (empty? deps) nil (vec deps))]
        doc-str
        (list (sym-fn fn-sym)  fn-args)))))

(def input-meta-kw :com.stuartsierra.flow/inputs)

(defn- required-args [flow]
  (mapcat (comp input-meta-kw meta) (vals flow)))

;; We need this function in the mk-workflow-fn macro, and therefore it needs to be public?
(defn external-args [flow]
  (clojure.set/difference (set (required-args flow)) (set (keys flow))))

(defn- internal-args [flow]
  (clojure.set/difference (set (required-args flow)) (set (external-args flow))))

(defn calc-steps [flow]
  (let [ext-args (external-args flow)
        inputs (set (mapcat (comp input-meta-kw meta) (vals flow)))]
    (reduce (fn [acc [k v]]
              (let [m (meta v)
                    deps (remove ext-args (input-meta-kw m))
                    args-deps (input-meta-kw m)
                    aliases (:aliases m)
                    args-names (map #(get aliases % %) args-deps)
                    step-deps (remove (external-args flow) args-deps)
                    intermediate? (not (nil? (inputs k)))]
                (assoc acc k { :args-deps args-deps :args-names args-names :step-deps step-deps :deps deps :intermediate? intermediate? :fn v }))) {} flow)))

(defn- mk-taps [deps internal-args]
  (map
    (fn [k]
      (let [k-sym (symbol (name k))]
        (if (internal-args k)
          (list `hfs-seqfile (sym-dir k-sym))
          k-sym)))
    deps))

(defn- flow-graph [flow]
  (reduce (fn [graph [output f]]
            (reduce (fn [g input] (dep/depend g output input))
                    graph (input-meta-kw (meta f))))
          (dep/graph) flow))

(defn- sort-graph [flow steps]
  (let [sorted-keys (dep/topo-sort (flow-graph flow))]
    (map #(list % (% steps)) (filter (set (keys steps)) sorted-keys))))

(defn- pp-step [step]
  (let [[name & [[conf doc-str & [body]]]]  step]
    [(str name " (" conf)
     (str "  " doc-str)
     (str "  " body ")")]))

;;; Public API

(defn flow->workflow [flow]
  (->> (calc-steps flow)
    (sort-graph flow)
    (mapcat (fn [[k v]]
             (let [f-m (-> v :fn meta)
                   f (str (:ns f-m) "/" (name (:name f-m)))
                   fn-doc (:doc f-m)
                   dir (sym-dir k)
                   step-deps (map sym-step (:step-deps v))
                   arg-keys (:args-deps v)
                   args-names (:args-names v)
                   args-taps (mk-taps arg-keys (internal-args flow))
                   f-args (zipmap args-names args-taps)]
               (if (:intermediate? v)
                (mk-intermediate-step {:name k :deps step-deps :fn-doc fn-doc :fn-sym f :fn-args f-args})
                (mk-endpoint-step {:name k :deps step-deps :fn-doc fn-doc :fn-sym f :fn-args f-args})))))))

(defn pp-workflow
  "Pretty prints a complete checkpoint workflow for debugging"
  [flow]
  (->> (flow->workflow flow)
       (partition 2)
       (mapcat pp-step)
       (clojure.string/join "\n")
       println))

(defn fns-to-flow
  "Given a list of flow functions a graph will be generated that can be printed or executed"
  [& fns]
  (apply hash-map (mapcat
                   (fn [f]
                     (let [m (meta f)
                           name (keyword (:name m))
                           v (var-get f)
                           v (with-meta v (assoc (meta v) :name name :ns (:ns m)))
                           ]
                       [name v])) fns)))

;; TODO remove duplication between rename-meta,rename-meta-all,update-flow,update-flow-all
(defn rename-meta-all
  "Rename steps and all inputs of a graph"
  [v inner-deps func]
  (let [m (meta v)
        inputs (input-meta-kw  m)
        new-inputs (map func inputs)
        aliases (zipmap new-inputs inputs)]
        (with-meta v (assoc m input-meta-kw new-inputs :aliases aliases))))

(defn rename-meta
  "Rename steps of a graph"
  [v inner-deps func]
  (let [m (meta v)
        [inner-inputs external-inputs] (split-with inner-deps (input-meta-kw  m))
        new-inputs (map func inner-inputs)
        aliases (zipmap new-inputs inner-inputs)
        all-inputs (set (concat new-inputs external-inputs))]
        (with-meta v (assoc m input-meta-kw all-inputs :aliases aliases))))

(defn update-flow
  "Update step names of a graph"
  [graph func]
  (let [inner-deps (set (keys graph))]
    (reduce (fn [acc [k v]] (assoc acc (func k) (rename-meta v inner-deps func))) {} graph)))

(defn update-flow-all
  "Update step names and input names of a graph
  Useful for reusing a graph structure with different inputs
  "
  [graph func]
  (let [inner-deps (set (keys graph))]
    (reduce (fn [acc [k v]] (assoc acc (func k) (rename-meta-all v inner-deps func))) {} graph)))

(declare flow-fn)

(defmacro mk-workflow-fn
  "Create a function that can run a workflow
  Accepts keyword arguments"
  [flow]
  (let [workflow# (eval `(flow->workflow ~flow))
        external-symbols# (mapv (comp symbol name) (eval `(external-args ~flow)))]
    ;; Use flow-fn to generate a function that matches keywords and asserts on presence
    ;; The :inputs metadata is also useful for introspection
    `(f/flow-fn ~external-symbols#
      (assert ~external-symbols#)
      (checkpoint/workflow ["/tmp/cascalog-checkpoint"] ~@workflow#))))

;; Steal functions from Flow library, without the need for people to manage the dependency themselves

;; TODO can we make a defn-like form of flow-fn?
(defmacro flow-fn
  "Returns a function for use in a flow. The function will take a
    single map argument. inputs is either a destructuring map form or a
    vector of symbols to destructure as with {:keys [...]}."
  [inputs & body]
  `(f/flow-fn ~inputs ~@body))

(def dot
  "Prints a representation of the flow to standard output,
  suitable for input to the Graphviz 'dot' program. Options are
  key-value pairs from:

    :graph-name   string/symbol/keyword naming the graph. Must not be
                  a Graphiviz reserved word such as \"graph\"."
  f/dot)

(def write-dotfile
  "Writes a Graphviz dotfile for a Flow. options are the same as for
    'dot'."
  f/write-dotfile)
