(ns adgoji.cascalog.cli
  (:require [adgoji.cascalog.graph :as graph]
            [clojure.java.shell :as shell]
            [plumbing.fnk.pfnk :as pfnk]
            [cascalog.api :as cascalog]
            [adgoji.cascalog.cli.hdfs :as hdfs]
            [clojure.tools.cli :as cli]))

;; mk-tap

(defmulti mk-tap (fn [scheme & _] scheme))

(defmethod mk-tap nil [& _]
  nil)

(defmethod mk-tap :default [scheme & _]
  (println "Tap" scheme "not recognized")
  (System/exit 1))

(defn keywordize-options [options]
  (into {} (for [[k v] options] [k (keyword v)])))

(defmethod mk-tap "hfs-seqfile" [_]
  {:tap-fn (fn [{:keys [path params]}]
             (apply cascalog/hfs-seqfile path (apply concat params)))
   :allowed-types [:sink :source]
   :params-transform keywordize-options
   :sink-options {:sinkmode {:default :keep
                             :vals {:keep "Default, errors when already exists"
                                    :update "Updates"
                                    :replace "Replaces the "}}}
   :validation (fn [{:keys [path params credentials type]}]
                 (when (and (= type :sink)
                            (not (#{:update :replace} (:sinkmode params)))
                            (hdfs/file-exists? path))
                   (str  "the given path \"" path "\" already exists")))})


(defmethod mk-tap "hfs-textline" [_]
  (assoc (mk-tap "hfs-seqfile")
    :tap-fn (fn [{:keys [path params]}]
                    (apply cascalog/hfs-textline path params))))

(defmethod mk-tap "stdout" [_]
  {:tap-fn (fn [& _] (cascalog/stdout)) :allowed-types [:sink]})

(defn unsupported-options-error [conf unsupported-options]
  (str "the parameters "
       (clojure.string/join ","
                            (map (comp pr-str name key)
                                 unsupported-options))
       " are not supported. Choose from "
       (clojure.string/join ", "
                            (map (fn [[k v]]
                                   (str (name k) "="
                                        (clojure.string/join "|"
                                                             (map name (keys (:doc v))))))
                                 conf) )))


(defn tap-type [option-name]
  (cond
   (.endsWith option-name "sink-tap")   :sink
   (.endsWith option-name "source-tap") :source
   :else :unknown))

(def reserved-hdfs-schemes
  #{"s3n" "s3"})

(defn parse-tap-str [tap-str]
  {:pre [(not (nil? tap-str))]}
  (let [[tap-str params-str] (clojure.string/split tap-str #"\?" 2)
        params (if params-str (clojure.walk/keywordize-keys (into {} (map #(clojure.string/split % #"=")
                                                                          (clojure.string/split params-str #"&")))) {})
        [scheme path] (clojure.string/split tap-str #":" 2)
        [extension & dir-name] (reverse (clojure.string/split tap-str #"\."))
        [tap-format path] (cond
                           (and scheme path
                                (not (reserved-hdfs-schemes scheme))) [scheme path]
                                (and dir-name extension) [extension tap-str]
                                :else [scheme nil])]
    {:format tap-format :path path :params params}))

(defn assert-tap-config [config]
  (let [valid-types #{:sink :source}
        types (:allowed-types config)]
    (assert (fn? (:tap-fn config))
            "expected key :tap-fn with a function as value")

    (assert (seq (filter valid-types types))
            "expected key :types with one or both [:sink :source]")

    (assert (empty? (remove valid-types types))
            (str "wrong value(s) for key :types " (remove valid-types types)))))

(defn validate-options [tap-name options-conf options]
  (remove nil?
          (map (fn [[ opt opt-val] ]
                 (let [allowed-vals (set (keys (get-in options-conf [opt :vals])))]
                   (if(empty? allowed-vals)
                     (str opt " is not a valid option")
                     (if-not (allowed-vals opt-val)
                       (str opt-val " is not allowed for "  opt
                            ". Choose one of [" (clojure.string/join ", " allowed-vals) "]"))))) options)))

(defn validate-format [tap-name allowed-types type format]
  (if ((set (conj (or allowed-types []) :unknown))  type)
    []
    [(str "tap type " format  " is not allowed as " type ", only the following are allowed "  allowed-types)]))

(defn mk-tap* [tap tap-name {:keys [format type path params] :or {type :unknown}}]
  (let [options-conf  (merge (tap :params) (tap :sink-options) (tap :source-options))
        format-errors (validate-format tap-name (:allowed-types tap) type format)
        params ((tap :params-transform identity) params)
        errors (concat format-errors (validate-options tap-name options-conf params))]
    (if (seq errors)
      { :errors errors}
      {:path path :params params :type type
       :tap-fn (:tap-fn tap)
       :validation-fn #((tap :validation (constantly nil))
                        {:format format :type type :path path :params params})})))

(defn parse-tap [tap-name tap-str]
  (let [{:keys [format] :as tap-params} (parse-tap-str tap-str)]
    (mk-tap* (mk-tap format) tap-name (assoc tap-params :type (tap-type tap-name)))))

(defn parse-tap-fn [tap-name]
  (fn [tap-str] (parse-tap tap-name tap-str)))

(defn str->tap
  ([tap-str] (str->tap tap-str :unknown))
  ([tap-str type]
     (let [{:keys [format] :as tap-params } (parse-tap-str tap-str)
           tap (mk-tap* (mk-tap format))]
       ((:tap-fn tap) tap-params))))

(defn- calculate-cli-options [inputs-kws]
  (cons
   ["--mode" "The run mode. Can be execute (default), validation (e.g. useful for Lemur), preview, dot or debug" :default :execute]
   (map (fn [input-kw]
          (let [option-name (name input-kw)
                option-flag (str "--" option-name)]
            (if (.endsWith option-name "-tap")
              [option-flag (str "Cascading tap for option " option-name) :parse-fn (parse-tap-fn option-name)]
              [option-flag (str "Value for option " option-name)]))) inputs-kws)))

(defn print-usage [switches-usage]
  (println)
  (println "Manual")
  (println "-------------")
  (println (str "Cascading tap arguments can be one of the following schemes: " (clojure.string/join ", " (remove #{nil :default} (keys (methods mk-tap))))))
  (println      "Use the format <tap-scheme>:<tap-args> or <tap-args-without-extension>.<extension> to indicate the tap format")
  (println)
  (println switches-usage))

(defn check-missing-options! [opts banner]
  (let [empty-inputs (map (comp (partial str "--") name) (keys (filter (fn [[k v]] (nil? v)) opts)))]
    (when (seq empty-inputs)
      (println (str "The following arguments are absent or empty " (clojure.string/join ", " empty-inputs)))
      (print-usage banner)
      (System/exit -1))))

;; ## Run modes
;;
;; A job can run in different modes. The default one is to execute the job as a workflow. Other options are
;; to visualize, validate and debug a job. These options can be extended by expanding the multimethod run-mode

(defmulti run-mode (fn [{:keys [opts]}] (keyword (:mode opts))))

(defmethod run-mode :validation [{:keys [opts banner]}]
  (check-missing-options! opts banner)
  (println "Input options are valid"))

(defn print-errors+banner [errors banner]
  (println "Error for options:")
  (doseq [[option {errs :errors}] errors]
    (println (str "\t\t --" (name option) ":"))
    (println (str "\t\t\t" (clojure.string/join ", " errs))))
  (println banner)
  (System/exit 1))

(defn println+error-exit [& s]
  (apply println s)
  (System/exit 1))

(defmethod run-mode :execute [{:keys [opts banner callback]}]
  (check-missing-options! opts banner)
  (let [opts (dissoc opts :mode)]
    (if-let [errors (seq (filter (comp seq :errors val) opts))]
      (print-errors+banner errors banner)
      (if-let [validation-errors (seq (remove nil?
                                              (map (fn [[k v]] ((get v :validation-fn (constantly nil)))) opts)))]
        (println+error-exit opts "found errors during tap validation " validation-errors)
        (callback (into {}
                         (map (fn [[k v]]
                                [k
                                 (if-let [tap-fn (:tap-fn v)]
                                   (tap-fn v)
                                   v)])
                              opts)))))))

(defmethod run-mode :dot [{:keys [fn-like]}]
  (graph/dot-compile fn-like))

(defmethod run-mode :preview [{:keys [fn-like]}]
  (let [tmp-path (str "/tmp/job-" (java.util.UUID/randomUUID))
        dot-path (str tmp-path ".dot")
        png-path (str tmp-path ".png")
        dot-str (with-out-str (graph/dot-compile fn-like))
        _ (spit dot-path dot-str)
        {dot? :exit error :error} (shell/sh "dot" "-Tpng" dot-path "-o" png-path)]
    (if dot?
      (do (println "opening" png-path "...")
          (shell/sh "open" png-path)
          (System/exit 0))
      (do (println "Error: graphviz not installed?" error)
          (System/exit 1)))))

;; TODO how can we make the printing of the workflow code pretty?
;;   This doesn't work as expected http://clojuredocs.org/clojure_core/clojure.pprint
(defmethod run-mode :debug [{:keys [fn-like]}]
  (println (graph/mk-workflow "/tmp/cascalog-checkpoint" fn-like))  )

(defmethod run-mode :default [{:keys [opts]}]
  (println "Error: run mode" (:mode opts) "not recognized")
  (System/exit 1))

(defn with-options [args options callback & [fn-like]]
  (let [[_ _ banner] (apply cli/cli [] options)
        [opts _trailing-args _banner]
        (try
          (apply cli/cli args options)
            (catch Exception e
              ;; Unfortunately tools.cli doesn't throw custom exception types we can catch
              (if (.. e getMessage (endsWith "is not a valid argument"))
                (do
                  (println "Error: " (.getMessage e))
                  (print-usage banner)
                  (System/exit -1))
                (throw e))))]
    (run-mode {:opts opts :callback callback :fn-like fn-like :banner banner})))

(defn with-job-options [args callback fn-like]
  (let [inputs (keys (pfnk/input-schema callback))]
    (with-options args (calculate-cli-options inputs) callback fn-like)))

(defmacro defjob [job-name fn-like]
  `(cascalog/defmain ~job-name [& args#]
     (let [fn-like# ~fn-like
           job-fn# (if (fn? fn-like#)
                     fn-like#
                     (graph/workflow-compile fn-like#))]
       (with-job-options args# job-fn# fn-like#))))
