(defproject adgoji/cascalog-graph "0.0.1-SNAPSHOT"
  :description "Graph implementation for Cascalog"
  :url "http://github.com/jeroenvandijk/cascalog-graph"
  :license { :name "Eclipse Public License"
             :url "http://www.eclipse.org/legal/epl-v10.html" }

  :jvm-opts ["-XX:MaxPermSize=128M"
             "-XX:+UseConcMarkSweepGC"
             "-Xms1024M" "-Xmx1048M" "-server"]

  :dependencies [
                 [com.stuartsierra/flow "0.1.0"]
                 [org.clojure/tools.namespace "0.2.2"]
                 
                 ;; FIXME There seems to be an issue with 0.1.0
                 [prismatic/plumbing "0.0.1"]


                 ;; REVIEW We only rely on some Cascalog symbols and during testing. Can we
                 ;; remove this to dev profile instead?
                 [cascalog "1.9.0"]
                 

                 [cascalog-checkpoint "0.2.0"]
                ]

  :profiles {
    :dev {
          :source-paths ["dev/src"]
          :plugins [
                     [lein-midje "3.0.1"]]

          :dependencies [
                         [midje "1.5.1"]

                         [org.clojure/clojure "1.4.0"]
                         [org.apache.hadoop/hadoop-core "0.20.2-dev" :exclusions [log4j org.slf4j/slf4j-log4j12 org.slf4j/slf4j-api commons-logging]]]
    }
  })
