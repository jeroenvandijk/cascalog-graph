(defproject adgoji/cascalog-graph "0.0.1"
  :description "Graph implementation for Cascalog"
  :url "http://github.com/jeroenvandijk/cascalog-graph"
  :license { :name "Eclipse Public License"
             :url "http://www.eclipse.org/legal/epl-v10.html" }

  :dependencies [
                  [com.stuartsierra/flow "0.1.0"]
                  [org.clojure/tools.namespace "0.2.2"]

                  [cascalog "1.9.0"]
                  [cascalog-checkpoint "0.2.0"]
                ]

  :profiles {
    :dev {
      :source-paths ["dev/src"]
      :dependencies [[org.clojure/clojure "1.4.0"]
                     [org.apache.hadoop/hadoop-core "0.20.2-dev" :exclusions [log4j org.slf4j/slf4j-log4j12 org.slf4j/slf4j-api]]]
    }
  })
