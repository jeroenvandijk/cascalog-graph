(ns adgoji.cascalog.cli.hdfs
 (:import [org.apache.hadoop.fs FileSystem]
          [org.apache.hadoop.conf Configuration]
          [org.apache.hadoop.fs Path]))

(defn hdfs []
  (FileSystem/get (Configuration.)))

(defn file-exists? [hdfs-path]
  (.exists (hdfs) (Path. hdfs-path)))
