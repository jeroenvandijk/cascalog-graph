(ns adgoji.cascalog.cli
  (:require [adgoji.cascalog.graph :as graph]
            [cascalog.api :refer [?- hfs-seqfile]]
            [clojure.java.shell :as shell]
            [plumbing.fnk.pfnk :as pfnk]
            [cascalog.api :as cascalog]
            [clojure.tools.cli :as cli]))

(defn tap-error [msg]
  (println msg)
  (System/exit 1))

;; ## Automatic tap detection
;; Taps can be recognized by a prefix or extension. This makes workflows more flexible
;; since the format doesn't matter. As long as the structure of the tap is the same. By
;; extending the multimethod `mk-tap` allow to add more taps.

(defmulti mk-tap (fn [scheme & _] scheme))

(defmethod mk-tap nil [& args]
  nil)

(defmethod mk-tap :default [scheme & args]
  (println "Tap" scheme "not recognized")
  (System/exit 1))

(defmethod mk-tap "hfs-seqfile" [_ {:keys [type]} args]
  ;; TODO check on directory existence if source or absence when sink
  (cascalog/hfs-seqfile args))

(defmethod mk-tap "hfs-textline" [_ {:keys [type]} args]
  (cascalog/hfs-textline args))

(defmethod mk-tap "stdout" [_ {:keys [type name]} _]
  (if (#{:sink :unknown} type)
    (cascalog/stdout)
    (tap-error (str "Tap " name " should can only be used as a sink"))))

(defn tap-type [option-name]
  (cond
    (.endsWith option-name "sink-tap")   :sink
    (.endsWith option-name "source-tap") :source
    :else :unknown))

(def reserved-hdfs-schemes
  #{"s3n" "s3"})

(defn parse-tap-str [tap-str]
  (let [[scheme args] (when tap-str (clojure.string/split tap-str #":" 2))
        [extension & dir-name] (when tap-str
                                 (reverse (clojure.string/split tap-str #"\.")))
        [tap-format args] (cond
                           (and scheme args
                                (not (reserved-hdfs-schemes scheme))) [scheme args]
                                (and dir-name extension)                   [extension tap-str]
                                :else [scheme nil])]
    {:format tap-format :args args}))

(defn- parse-tap-fn [tap-name]
  (fn [tap-str]
    (let [{:keys [format args]} (parse-tap-str tap-str)]
      (mk-tap format {:type (tap-type tap-name) :name tap-name} args))))

(defn- calculate-cli-options [inputs-kws]
  (cons
   ["--mode" "The run mode. Can be execute (default), validation (e.g. useful for Lemur), preview, dot or debug" :default :execute]
    (map (fn [input-kw]
      (let [option-name (name input-kw)
            option-flag (str "--" option-name)]
            (if (.endsWith option-name "-tap")
              [option-flag (str "Cascading tap for option " option-name) :parse-fn (parse-tap-fn option-name)]
              [option-flag (str "Value for option " option-name)]))) inputs-kws)))

(defn unwrap-fns [opts]
  (apply hash-map
         (mapcat (fn [[k v]]
                   (if (fn? v)
                     [k (v)]
                     [k v])) opts)))

(defn print-usage [switches-usage]
  (println)
  (println "Manual")
  (println "-------------")
  (println (str "Cascading tap arguments can be one of the following schemes: " (clojure.string/join ", " (keys (methods mk-tap)))))
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

(defmethod run-mode :execute [{:keys [opts banner callback]}]
  (check-missing-options! opts banner)
  (callback (unwrap-fns opts)))

(defmethod run-mode :dot [{:keys [fn-like]}]
  (graph/dot-compile fn-like))

(defmethod run-mode :preview [{:keys [fn-like]}]
  (let [tmp-path (str "/tmp/job-" (java.util.UUID/randomUUID))
        dot-path (str tmp-path ".dot")
        png-path (str tmp-path ".png")
        dot-str (with-out-str (graph/dot-compile fn-like))]
    (spit dot-path dot-str)
    (let [{dot? :exit error :error} (shell/sh "dot" "-Tpng" dot-path "-o" png-path)]
      (if dot?
        (do (println "opening" png-path "...")
            (shell/sh "open" png-path)
            (System/exit 0))
        (do (println "Error: graphviz not installed?" error)
             (System/exit 1))))))

;; TODO how can we make the printing of the workflow code pretty?
;;   This doesn't work as expected http://clojuredocs.org/clojure_core/clojure.pprint
(defmethod run-mode :debug [{:keys [fn-like]}]
  (println (graph/mk-workflow "/tmp/cascalog-checkpoint" fn-like))  )

(defmethod run-mode :default [{:keys [opts]}]
  (println "Error: run mode" (:mode opts) "not recognized")
  (System/exit 1))

(defn with-options [args options callback & [fn-like]]
  (let [[_ _ banner] (apply cli/cli [] options)
        [opts _trailing-args _banner] (try
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
