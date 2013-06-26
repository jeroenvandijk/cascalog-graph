(ns cascalog-graph.cli-test
  (:require [adgoji.cascalog.graph :as g]
            [adgoji.cascalog.cli.tap :as tap]
            [cascalog.api :as cascalog :refer :all]
            [adgoji.cascalog.cli :as cli :refer :all]
            [midje.sweet :refer :all]))

(tabular "destructuring tap-uri's"
         (fact (tap/parse-tap-str ?tap-str) => {:format ?scheme :params ?params :path ?path})
         ?tap-str                                   ?scheme    ?path                     ?params
         "hfs-json:path/to/tap"                     "hfs-json" "path/to/tap"             {}
         "path/to/tap.hfs-json"                     "hfs-json" "path/to/tap.hfs-json"    {}
         "hfs-json:s3:path/to/tap"                  "hfs-json" "s3:path/to/tap"          {}
         "s3:path/to/tap.hfs-json"                  "hfs-json" "s3:path/to/tap.hfs-json" {}
         "hfs-json:path/to/tap?sinkmode=replace"    "hfs-json" "path/to/tap"             {:sinkmode "replace"}
         "path/to/tap.hfs-json?sinkmode=replace"    "hfs-json" "path/to/tap.hfs-json"    {:sinkmode "replace"}
         "hfs-json:s3:path/to/tap?sinkmode=replace" "hfs-json" "s3:path/to/tap"          {:sinkmode "replace"}
         "s3:path/to/tap.hfs-json?sinkmode=replace" "hfs-json" "s3:path/to/tap.hfs-json" {:sinkmode "replace"})



(tabular
 "Asserting incorrect tap configurations fails"
 (fact
  (tap/assert-tap-config ?config) => ?res)
 ?config ?res
 {} (throws AssertionError)
 {:tap-fn (fn [] ) :allowed-types []} (throws AssertionError)
 {:tap-fn (fn [] ) :allowed-types [:wrong :source]} (throws AssertionError)
 {:tap-fn (fn []) :allowed-types [:source :sink]}
 )

(fact "Asserting correct tap configurations does not fail"
      (tap/assert-tap-config {:tap-fn (fn []) :allowed-types [:source :sink]}) =not=>
      (throws AssertionError))

(tabular
 "mk-tap works for defaults"
 (fact
  (tap/assert-tap-config (tap/mk-tap ?format)) =not=> (throws AssertionError))
 ?format
 "hfs-seqfile"
 "hfs-textline"
 "stdout")

(fact
 "Unsupported options"
 (tap/unsupported-options-error {:foo {:doc {:bar "bar doc" :baz :doc}}} {:z 1 :x 2}) =>
 "the parameters \"z\",\"x\" are not supported. Choose from foo=bar|baz")

(defn test-tap [config uri]
  (dissoc (tap/mk-tap* config uri {}) :validation-fn :tap-fn))

(let [tap-conf {:options { :a { :vals { :1 "an option"}}}}]
  (fact "applies params-transform correctly"
        (:args (test-tap tap-conf "foo.test-plain?a=1")) => {:path "foo.test-plain" :params { :a :1} :format "test-plain"})
  (fact "complains about invalid options"
        (tap/validate-tap-options (test-tap tap-conf "foo.test-plain?c=1")) => ":c is not a valid option")
  (fact "complains about invalid option values"
        (tap/validate-tap-options (test-tap tap-conf "foo.test-plain?a=2")) => ":2 is not allowed for :a. Choose one of [:1]"))
