(defproject adgoji/cascalog-graph "0.2.6-SNAPSHOT"
  :description "Graph implementation for Cascalog"
  :url "http://github.com/jeroenvandijk/cascalog-graph"
  :license { :name "Eclipse Public License"
             :url "http://www.eclipse.org/legal/epl-v10.html" }

  :jvm-opts ["-XX:MaxPermSize=128M"
             "-XX:+UseConcMarkSweepGC"
             "-Xms1024M" "-Xmx1048M" "-server"]

  :dependencies [
                 ;; FIXME There seems to be an issue with 0.1.0
                 [prismatic/plumbing "0.0.1"]
                 
                 [cascalog/cascalog-core "2.1.0"]
                 [cascalog/cascalog-checkpoint "2.1.0"]

                 ;; Command line args processing
                 [org.clojure/tools.cli "0.2.2"]
                ]

  :profiles {
    :dev {
          :source-paths ["dev"]
          :plugins [
                     [lein-midje "3.0.1"]]

          :dependencies [
                         [midje "1.5.1" :exclusions [org.codehaus.plexus/plexus-utils org.clojure/math.combinatorics]]
                         ;; In order to prevent internal midje conflicts
                         [org.codehaus.plexus/plexus-utils "3.0"]
                         [cascalog/midje-cascalog "2.1.0" :exclusions [midje]]
                         [org.clojure/clojure "1.5.1"]
                         [org.apache.hadoop/hadoop-core "0.20.2-dev" :exclusions [log4j org.slf4j/slf4j-log4j12 org.slf4j/slf4j-api commons-logging]]]
    }
  })
