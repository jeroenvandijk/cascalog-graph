(ns cascalog-graph.cli-test
  (:require [adgoji.cascalog.cli :as g :refer :all]
            [cascalog.api :as cascalog :refer :all]
            [adgoji.cascalog.cli :as cli]
            [midje.sweet :refer :all]))

(tabular "destructuring tap-uri's"
         (fact (parse-tap-str ?tap-str) => {:format ?scheme :params ?params :path ?path})
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
 "`validate-tap-constructor "
 (fact
  (assert-tap-config ?config) => ?res)
 ?config ?res
 {} (throws AssertionError)
 {:tap-fn (fn [] ) :allowed-types []} (throws AssertionError)
 {:tap-fn (fn [] ) :allowed-types [:wrong :source]} (throws AssertionError)
 {:tap-fn (fn []) :allowed-types [:source :sink]}
 )

(fact ""
      (assert-tap-config {:tap-fn (fn []) :allowed-types [:source :sink]}) =not=>
      (throws AssertionError))

(tabular
 "mk-tap works for defaults"
 (fact
  (assert-tap-config (cli/mk-tap ?format)) =not=> (throws AssertionError))
 ?format
 "hfs-seqfile"
 "hfs-textline"
 "stdout")


(fact
 "Unsupported options"
 (unsupported-options-error {:foo {:doc {:bar "bar doc" :baz :doc}}} {:z 1 :x 2}) =>
 "the parameters \"z\",\"x\" are not supported. Choose from foo=bar|baz")

(defn test-tap [config name uri]
  (dissoc (cli/mk-tap* config name uri) :validation-fn))

(fact "Should parse params correctly"
      (test-tap  {:options { :a { :vals { "1" "an option"}}}} "foo-tap" "foo.test-plain?a=1")
       => {:path "foo.test-plain" :type :unknown :params { :a "1"}})

(let [tap-conf {:params-transform cli/keywordize-options
                :options { :a { :vals { :1 "an option"}}}}]
  (fact "applies params-transform correctly"
        (test-tap tap-conf "foo-tap" "foo.test-plain?a=1") => {:path "foo.test-plain" :type :unknown :params { :a :1}})
  (fact "complains about invalid options"
        (test-tap tap-conf "foo-tap" "foo.test-plain?c=1") => {:errors {:c "is not a valid option"}})
  (fact "complains about invalid option values"
        (test-tap tap-conf "foo-tap" "foo.test-plain?a=2") => {:errors {:a "\":2\" is not allowed for :a. Choose one of [:1]"}}))
