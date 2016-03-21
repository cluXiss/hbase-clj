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

(defmacro defhbase 

  "Defines a HBase Connection config with specified var name according to the options, 
   params should be pairs, e.g: 
   (defhbase main-hb
   \"hbase.zookeeper.quorum\" \"localhost\"
  ...
   ...)"


  [name & options]

  `(def ^HBaseConfiguration ~name 
     (gen-config ~@options)))

(defn connect!
  [cfg]
  (if-not (@connections cfg) 
    (swap! connections 
           assoc cfg
           (HConnectionManager/createConnection cfg)))
  (@connections cfg))

(defrecord HBTable-schema
  [hb table-name id-type families])

(defmacro def-hbtable 

  "defines a htable \"schema\", 
   types could be one of #{:int :long :string :keyword :raw}
   and could create custom types via codec.defcodec
   families could be like: 

   --------------Snippet--------------
   ...
   ....
   :info {:--ktype :keyword
          :--vtype :string 
          :age :long}

   :following {:--ktype :string
               :--vtype :string}
   ....
   ...
   -----------------------------------

   where `--ktype` means the type of all keys in the family, default as `:keyword`
         `--vtype` means the default type of values in the family, default as `string`
         and other k-v stands for a specified value type given a key, instead of the `--vtype`"

  [table {:keys [id-type table-name hbase]
          :or {id-type :string}}
   & families] 

  `(def ^HBTable-schema ~table 
     (HBTable-schema. 
       (or ~hbase @latest-config)
       (or ~table-name (name ~table))
       ~id-type
       (hash-map ~@families))))

(defmacro with-table 
  [table & form]
  `(let [hb#         (:hb ~table)
         table-name# (:table-name ~table)]
     (binding 
       [^HTableInterface *table* (.getTable (connect! hb#) 
                                            (Bytes/toBytes table-name#))
        *schema* (select-keys ~table [:id-type :families])]
       ~@form
       (.close *table*))))
