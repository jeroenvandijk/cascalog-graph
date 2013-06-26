(ns adgoji.cascalog.cli.tap
  (:require [adgoji.cascalog.graph :as graph]
            [clojure.java.shell :as shell]
            [plumbing.fnk.pfnk :as pfnk]
            [cascalog.api :as cascalog]
            [adgoji.cascalog.cli.hdfs :as hdfs]))

(defn assert-tap-config [config]
  (let [valid-types #{:sink :source}
        types (:allowed-types config)]
    (assert (fn? (:tap-fn config))
            (str "expected key :tap-fn with a function as value instead of " (pr-str (:tap-fn config))))

    (assert (seq (filter valid-types types))
            "expected key :allowed-types with one or both [:sink :source]")

    (assert (empty? (remove valid-types types))
            (str "wrong value(s) for key :types " (remove valid-types types)))))

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

(def reserved-hdfs-schemes
  #{"s3n" "s3"})

(defn validate-format [allowed-types type format]
  (if ((set (conj (or allowed-types []) :unknown))  type)
    []
    [(str "tap type " format  " is not allowed as " type ", only the following are allowed "  allowed-types)]))

(defn validate-options [options-conf options]
  (remove nil?
          (map (fn [[opt opt-val] ]
                 (let [allowed-vals (set (keys (get-in options-conf [opt :vals])))]
                   (if (empty? allowed-vals)
                     (str opt " is not a valid option")
                     (if-not (allowed-vals opt-val)
                       (str opt-val " is not allowed for "  opt
                            ". Choose one of [" (clojure.string/join ", " allowed-vals) "]"))))) options)))

(defn keywordize-options [options]
  (into {} (for [[k v] options] [k (keyword v)])))

(defn validate-tap-options [{{:keys [format type path params] :or {type :unknown}} :args :as tap}]
  (let [options-conf (merge (tap :options) (tap :sink-options) (tap :source-options))
        errors (seq (remove nil?
                            (concat
                             (validate-format (:allowed-types tap) type format)
                             (validate-options options-conf params))))]
    (if errors
      (clojure.string/join "\n" errors))))

;; multi method that can transform a
(defmulti mk-tap identity)

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

(defn mk-tap*
  ([uri opts]
     (let [tap-config (parse-tap-str uri)]
       (mk-tap* (mk-tap (:format tap-config)) uri opts)))
  ([tap-config uri opts]
     (let [transformed-params (tap-config :params-transform keywordize-options)
           args (update-in (parse-tap-str uri) [:params] transformed-params)]
       (assoc tap-config :args (merge opts args)))))

;; ---------------
;; mk-tap
;; ---------------

;; Default to a validation function that gives errors
(defmethod mk-tap :default [format]
  {:validation (constantly (str "Tap " format  " not recognized"))})

(defmethod mk-tap "hfs-seqfile" [_]
  {:tap-fn (fn [{:keys [path params] :as args}]
             (apply cascalog/hfs-seqfile path (apply concat params)))
   :allowed-types [:sink :source]
   :params-transform keywordize-options
   :sink-options {:sinkmode {:default :keep
                             :vals {:keep "Default, errors when already exists"
                                    :update "Updates"
                                    :replace "Replaces the "}}}
   :validation (fn [{:keys [path params credentials type] :as foo}]
                 (let [exists? (hdfs/file-exists? path)]
                   (cond
                    (and (= type :sink)
                         (not (#{:update :replace} (:sinkmode params)))
                         exists?)
                    (str  "the given path \"" path "\" already exists")

                    (and (= type :source) (not exists?))
                    (str  "the given path \"" path "\" doesn't exist")

                    :else nil)))})


(defmethod mk-tap "hfs-textline" [_]
  (assoc (mk-tap "hfs-seqfile")
    :tap-fn (fn [{:keys [path params]}]
              (apply cascalog/hfs-textline path params))))

(defmethod mk-tap "stdout" [_]
  {:tap-fn (fn [& _] (cascalog/stdout)) :allowed-types [:sink]})
