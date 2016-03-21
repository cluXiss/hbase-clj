(ns hbase-clj.core
  (:require 
    (hbase-clj
      [driver :refer :all]
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

(declare get-putter get-getter get-deleter)
(defn validate-tables []
  (if-not (and *table* *schema*)
     (throw 
       (Exception.
         "table and schema must be specified before any operation"))))

(defn put-data!
  "(put-data! <id> <attrs>, <id> <attrs> .....)
   attrs should be a hashmap of <family: col-attrs>
   col-attrs should be a hashmaps of <col: val>"
  [& records]
  (validate-tables)
  (println *table*)
  (.put *table* 
        (vec 
          (for [[id attr-map] (partition 2 records)] 
            (get-putter id attr-map)))))

(defn get-data
  "(get-data <id> <attrs>, <id> <attrs> ....)
   attrs should be vector of <family> or [<family> [<attr> <attr> ....]],
   e.g: [[:info [:age :name]] :follow]
   "
  [& constraints]
  (validate-tables)
  (.get *table*
        (vec 
          (for [[id attrs] (partition 2 constraints)] 
            (get-getter id attrs)))))

(defn delete-data!
  "(get-data <id> <attrs>, <id> <attrs> ....)
   attrs should be vector of <family> or [<family> [<attr> <attr> ....]],
   e.g: [[:info [:age :name]] :follow]
   "
  [& constraints]
  (validate-tables)
  (.delete *table*
           (into-array 
             (for [[id attrs] (partition 2 constraints)] 
               (get-deleter id attrs)))))

(defn scan []
  (let [^Scan scanner (Scan.)]

    (validate-tables)

    (let [{:keys [id-type families]} *schema*

          res (atom {})]
      #_(doseq [r (.getScanner *table* scanner)]
        (doseq [^KeyValue kv (.list r)]
          (let [id (decode id-type (.getRow kv))

                family (decode :keyword (.getFamily kv))

                attr (decode (get-in families [family :--ktype]) 
                             (.getQualifier kv))
                value (decode 
                        (get-in families [family attr]
                                (get-in families [family :--vtype]))
                        (.getValue kv))
                ts (.getTimestamp kv)]

            (if-not (@res id)
              (swap! res assoc-in [id :id] id))

            (swap! res 
                   update-in [id family] 
                   update-in [attr]
                   #(conj (or % []) [value ts])))))
      @res)))

(defn- get-putter 
  [id attr-map]
  (let [{:keys [id-type families]} *schema*
        ^Put putter (Put. (encode id-type id))]
    (doseq [[family attr-map] attr-map]
      (let [typemap (families family)]
        (doseq [[k v] attr-map]
          (.add putter 
                (encode :keyword family) 
                (encode (:--ktype typemap) k)
                (encode (get typemap k (:--vtype typemap)) v)))))

    putter))

(defn- get-getter 
  [id attrs]
  (let [{:keys [id-type families]} *schema*
        ^Get getter (Get. (encode id-type id))
        addcolumn (fn [f c]
                    (.addColumn 
                      getter 
                      (encode :keyword f) 
                      (encode (get-in families [f c] 
                                      (get-in families [f :--ktype]))
                              c)))]

    (doseq [d attrs]
      (if (coll? d)
        (let [[f c] d]
          (if (coll? c)
            (doseq [c c] (addcolumn f c))
            (addcolumn f c)))
        (.addFamily getter 
                    (encode :keyword d))))

    getter))

(defn- get-deleter 
  [id attrs]
  (let [{:keys [id-type families]} *schema*
        ^Delete deleter (Delete. (encode id-type id))
        addcolumn (fn [f c]
                    (.addColumn 
                      deleter 
                      (encode :keyword f) 
                      (encode (get-in families [f c] 
                                      (get-in families [f :--ktype]))
                              c)))]

    (doseq [d attrs]
      (if (coll? d)
        (let [[f c] d]
          (if (coll? c)
            (doseq [c c] (addcolumn f c))
            (addcolumn f c)))
        (.addFamily deleter (encode :keyword d))))

    deleter))
