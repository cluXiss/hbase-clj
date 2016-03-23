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

(defn- proc-result
  [with-versions? id-type families r]
  [(decode id-type (.getRow r))
   (apply merge-with 
          (if with-versions? 
            (partial merge-with concat)
            merge)
          (for [^KeyValue kv (.list r)]
            (let [family (keyword (Bytes/toString (.getFamily kv)))
                  attr   (decode (get-in families [family :--ktype])
                                 (.getQualifier kv))
                  v      (decode (get-in families [family attr]
                                         (get-in families [family :--vtype]))
                                 (.getValue kv))
                  ts     (.getTimestamp kv)]
              {family {attr (if with-versions?
                              [{:val v :timestamp ts}]
                              v)}})))])


(defn put-data!
  "usage: 
   (get-data [id, attrs] [id, attrs] ....)
   attrs should be a hashmap of <family-> col-attrs>
   col-attrs should be a hashmaps of <col-> val>"
  [& records]
  (validate-tables)
  (.put *table* 
        (vec 
          (for [[id attr-map] records] 
            (get-putter id attr-map)))))

(defn get-data
  "usage:
   (get-data <id|attrs> <id|attrs> ....)
   (get-data :with-versions ...)
   <id|attrs> could be <id> or [<id> <attrs>]
   <attrs> should be vector of <family> or [<family> [<attr> <attr> ....]],
   e.g: 
       (get-data \"001\" 
                 [\"002\" [:info]]
                 [\"003\" [[:info [:age :name]] :follow]])
   Returns a seq of vec: [id, record]
   where every record as: {family-> {col-> val}}
   if passed `:with-versions` as the first arg, val would be a coll of {:val xxx :timestamp xxx}"
  [& rows]
  (validate-tables)
  (let [with-versions? (= (first rows) :with-versions)
        rows (if with-versions? (rest rows) rows)
        {:keys [id-type families]} *schema*]
    (map (partial proc-result with-versions? id-type families) 
         (.get *table*
               (vec 
                 (for [r rows] 
                   (if (coll? r) 
                     (get-getter (first r) (or (second r) [])
                                 (apply hash-map (drop 2 r)))
                     (get-getter r [] {}))))))))

(defn delete-data!
  "usage: 
   (delete-data [id, attrs] [id, attrs] ....)
   attrs should be vector of <family> or [<family> [<attr> <attr> ....]],
   e.g: [[:info [:age :name]] :follow]"
  [& constraints]
  (validate-tables)
  (.delete *table*
           (into-array 
             (for [[id attrs] (partition 2 constraints)] 
               (get-deleter id attrs)))))

(defn scan 
  "usage:
   (scan & options)
   (scan :with-versions & options)
   options could contain these keys:
   - :eager?       If set to true, return a hash-map similar to the result of `get`, if false, returns a lazy scanner to fetch results later.
   - :start-id     The ID the htable-scan starts from
   - :stop-id      The ID the htable-scan stops at
   - :cache-size   The number of rows fetched when getting a \"next\" item, only takes effect when `eager?` is set to false
   - :small?       Determine if this is a \"small\" scan, see: https://hbase.apache.org/0.94/apidocs/org/apache/hadoop/hbase/client/Scan.html#setSmall(boolean)
   - :max-versions The max version to fetch on each record
   - :time-range   The time-range to fetch on each record
   - :attrs        Same as the definition of `<attrs>` in `(get-data ..)`"
  [& args]
  (let [with-versions? 
        (= :with-versions (first args)) 

        args ((if with-versions? rest identity) args)

        {:keys [start-id stop-id 
                cache-size small?
                max-versions time-range
                attrs eager?]}
        (apply hash-map args)

        {:keys [id-type families]} *schema*

        ^Scan scanner (Scan.) 
        addcolumn
        (fn [f c]
          (.addColumn 
            scanner 
            (encode :keyword f) 
            (encode (get-in families [f c] 
                            (get-in families [f :--ktype]))
                    c)))]

    (if start-id 
      (.setStartRow scanner (encode id-type start-id)))

    (if stop-id 
      (.setStopRow scanner (encode id-type stop-id)))

    (if time-range 
      (.setTimeRange 
        scanner
        (long (first  time-range))
        (long (second time-range))))

    (if cache-size 
      (.setCaching scanner cache-size))

    (if max-versions
      (.setMaxVersions scanner max-versions))

    (doseq [d attrs]
      (if (coll? d)
        (let [[f c] d]
          (if (coll? c)
            (doseq [c c] (addcolumn f c))
            (addcolumn f c)))
        (.addFamily scanner 
                    (encode :keyword d))))

    (let [^ResultScanner res (.getScanner *table* scanner)]
      (if eager? 
        (let [t (transient [])] 
          (loop []
            (if-let [r (.next res)]
              (do 
                (conj! t (proc-result with-versions? id-type families r))
                (recur))
              (persistent! t))))
        (letfn [(get-res [] 
                  (lazy-seq 
                    (if-let [r (.next res)]
                      (cons (proc-result with-versions? id-type families r)
                            (get-res)))))]
          (get-res))))))

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
  [id attrs {:keys [max-versions time-range]}]
  (let [{:keys [id-type families]} *schema*
        ^Get getter (Get. (encode id-type id))
        addcolumn (fn [f c]
                    (.addColumn 
                      getter 
                      (encode :keyword f) 
                      (encode (get-in families [f c] 
                                      (get-in families [f :--ktype]))
                              c)))]
    (if max-versions 
      (.setMaxVersions getter max-versions))
    (if time-range
      (.setTimeRange getter 
                     (long (first time-range)) 
                     (long (second time-range))))
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
                                      (get-in families [f :--vtype]))
                              c)))]

    (doseq [d attrs]
      (if (coll? d)
        (let [[f c] d]
          (if (coll? c)
            (doseq [c c] (addcolumn f c))
            (addcolumn f c)))
        (.addFamily deleter (encode :keyword d))))

    deleter))
