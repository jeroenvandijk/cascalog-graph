(ns adgoji.cascalog.cli
  (:require [adgoji.cascalog.graph :as graph]
            [adgoji.cascalog.checkpoint :as checkpoint]
            [adgoji.cascalog.cli.tap :as tap]
            [cascalog.api :as cascalog]

            [clojure.java.shell :as shell]
            [plumbing.fnk.pfnk :as pfnk]
            [clojure.tools.cli :as cli]))

;;
;; A job can run in different modes. The default one is to execute the job as a workflow. Other options are
;; to visualize, validate and debug a job. These options can be extended by expanding the multimethod run-mode.
;; The different run modes are defined below.

(defmulti run-mode (fn [{:keys [mode]}] mode))

(defn mk-cli-arg [graph-option]
  (let [opt-name (name graph-option)
        opt-description (if (.endsWith opt-name "-tap")
                          (str "Cascalog tap uri for " opt-name)
                          (str "Value for " opt-name))]
    [(str "--" opt-name) opt-description :default nil]))

(defn handle-result [banner success? msg]
  ;; TODO print banner?
  (println msg)
  (when-not success? (println banner))
  (System/exit (if success? 0 1)))

(defn validate-cli-args [graph-opts args]
  (let [graph-spec (map mk-cli-arg graph-opts)
        specs (conj graph-spec ["--mode" (str "the run mode for this job, Choose from "
                                              (clojure.string/join ", " (keys (dissoc (methods run-mode) :default))))
                                :parse-fn keyword :default :execute
                                ])
        ;; Run once without args to get the banner, we can do it again after failure
        [_ _ banner]  (apply cli/cli [] specs)
        [opts trailing-args _ errors :as foobar]
        ;; Below we try to extract cli options. Unfortunately tools.cli doesn't return all results
        ;; when it comes accross an unknown options. This stops us in providing full feedback
        (try
          (apply cli/cli args specs)
          (catch Exception e
            ;; TODO fix clojure.tools.cli api, as it throws exceptions before everything is returned
            ;;   and unfortunately tools.cli doesn't throw custom exception types we can catch
            (if (.. e getMessage (endsWith "is not a valid argument"))
              [{} nil nil (.getMessage e)]
              (throw e))))]
    (if errors
      (handle-result banner false errors)
      {:banner banner :opts opts :trailing-args trailing-args})))

;; --------------------------------------------
;;; Run modes
;; --------------------------------------------

(defn print-usage [switches-usage]
  (println)
  (println "Manual")
  (println "-------------")
  (println (str "Cascading tap arguments can be one of the following schemes: " (clojure.string/join ", " (remove #{nil :default} (keys (methods tap/mk-tap))))))
  (println      "Use the format <tap-scheme>:<tap-args> or <tap-args-without-extension>.<extension> to indicate the tap format")
  (println)
  (println switches-usage))

(defn handle-failure [msg banner]
  (println "Error: " msg)
  (print-usage banner)
  (System/exit -1))

(defn errors-str [errors]
  (with-out-str
   (println "Error for options:")
   (doseq [[option {errs :errors}] errors]
     (println (str "\t\t --" (name option) ":"))
     (println (str "\t\t\t" (clojure.string/join ", " errs))))))

(defn invalid-tap-options [opts]
  (remove nil?
          (map (fn [[k v]]

                 (if-let [error (and (:tap-fn v)
                                     (tap/validate-tap-options v))]
                   (str "option " k " got " error))) opts)))

(defn invalid-taps [opts]
  (remove nil?
          (map (fn [[k v]]
                 (let [validation-fn (:validation v)
                       error (and validation-fn (validation-fn (:args v)))]
                   (if error
                     (str "option " k " got " error)))) opts)))

(defn validate-options [opts]
  (if-let [missing (seq (keys (filter (comp nil? val) opts)))]
    [false (str "The following arguments are absent or empty " (clojure.string/join ", " (map (comp (partial str "--") name) missing)))]
    (if-let [invalid-tap-opts (seq (invalid-tap-options opts))]
      [false (str "invalid tap options " (clojure.string/join ",  " invalid-tap-opts))]
      (if-let [inv-taps (seq (invalid-taps opts))]
        [false (str "invalid taps " (clojure.string/join ",  " inv-taps))]
        [true "options are valid"]))))

;; Improve the way the errors are printed
(defn validate-options! [opts banner]
  (apply handle-result banner (validate-options opts)))

(defn mk-taps [opts {:keys [sink-options source-options]}]
  (let [no-sink-options? (empty? sink-options)]
    (into {} (map (fn [[k v]] [k (if (and v (.endsWith (name k) "-tap"))
                                  (tap/mk-tap* v
                                               {:type
                                                (cond
                                                 no-sink-options? :unknown
                                                 ((set sink-options) k) :sink
                                                 :else :source)})
                                  v)]) opts))))

(defmethod run-mode :validation [{:keys [opts banner graph-meta]}]
  (validate-options! (mk-taps opts graph-meta) banner))

(defmethod run-mode :execute [{:keys [input-options banner callback job-options]}]
  (let [tap-opts (mk-taps input-options (if-not (fn? callback) (graph/tap-options callback)))
        [success? msg] (validate-options tap-opts)]
    (when-not success?
      (handle-result banner success? msg))
    (let [args (into {} (map (fn [[k v]]
                                [k (if-let [tap-fn (:tap-fn v)]
                                     (tap-fn (:args v))
                                     v)])
                             tap-opts))]
      (if (fn? callback)
        (callback args) ;; Add job options here as well?
        ((graph/workflow-compile callback job-options) args))
      ;; Return nil
      nil)))

(defmethod run-mode :dot [{:keys [callback]}]
  (graph/dot-compile callback))

(defn preview-graph [graph]
  (let [tmp-path (str "/tmp/job-" (java.util.UUID/randomUUID))
        dot-path (str tmp-path ".dot")
        png-path (str tmp-path ".png")
        dot-str (with-out-str (graph/dot-compile graph))
        _ (spit dot-path dot-str)
        {exit-status :exit error :err} (shell/sh "dot" "-Tpng" dot-path "-o" png-path)]
    (if (zero? exit-status)
      (do (println "opening" png-path "...")
          (shell/sh "open" png-path)
          (System/exit 0))
      (do (println "Error: graphviz not installed?" error)
          (System/exit 1)))))

(defmethod run-mode :preview [{:keys [callback]}]
  (preview-graph callback))

;; TODO Add dependency graph here?
(defmethod run-mode :debug [{:keys [callback]}]
  (println "doesn't make sense anymore? what do you want to see?"))

(defmethod run-mode :default [{:keys [opts banner]}]
  (handle-result banner false (str "Error: run mode '" (:mode opts) "' not recognized")))

(defn run-cmd [job-fn job-options cli-args]
  (let [{:keys [errors banner opts trailing-args] :as cli-validation} (validate-cli-args (graph/fnk-input-keys job-fn) cli-args)]
    (run-mode {:callback job-fn
               :mode (:mode opts)
               :input-options opts :errors errors :banner banner
               :job-options job-options
               :trailing-args trailing-args})))

(defmacro defjob [job-name graph-like & {:keys [] :as options}]
  `(cascalog/defmain ~job-name [& args#]
     (run-cmd ~graph-like ~options args#)))
