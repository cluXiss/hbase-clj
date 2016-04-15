(ns hbase-clj.driver
  (:require 
    (hbase-clj
      [codec :refer :all]))
  (:import 
    (org.apache.hadoop.conf 
      Configuration)
    (org.apache.hadoop.hbase 
      HBaseConfiguration
      HColumnDescriptor
      HTableDescriptor
      KeyValue)
    (org.apache.hadoop.hbase.client
      Delete Get Put Scan
      Result ResultScanner 
      HBaseAdmin HTable 
      HConnection HConnectionManager)
    (org.apache.hadoop.hbase.util
      Bytes)))

(def ^:dynamic *table* nil)
(def ^:dynamic *schema* nil)
(def ^:dynamic *batch-mode?* false)
(def ^:dynamic *batch-ops* false)

(def connections (atom {}))
(def latest-config (atom nil))

(defn ^HBaseConfiguration gen-config 

  "Generate a HBase Connection config according to the options, 
   params should be pairs, e.g: 
   (gen-config 
     \"hbase.zookeeper.quorum\" \"localhost\"
     ...
     ...)"

  [& options]

  (let [^HBaseConfiguration conf (HBaseConfiguration/create)]
    (doseq [[k v] (partition 2 options)]
      (.set conf k v))
    (reset! latest-config conf)
    conf))

(defn connect!
  [cfg]
  (if-not (@connections cfg) 
    (swap! connections 
           assoc cfg
           (HConnectionManager/createConnection cfg)))
  (@connections cfg))
