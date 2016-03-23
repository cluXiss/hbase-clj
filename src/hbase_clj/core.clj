(ns hbase-clj.core
  (:refer-clojure :exclude [get])
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
      Delete Get Put Scan Increment
      Result ResultScanner 
      HBaseAdmin HTable 
      HConnection HConnectionManager)
    (org.apache.hadoop.hbase.util
      Bytes)))

(declare get-putter get-getter get-deleter get-increr)

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


(defn put!
  "Put into HBase Table
   Should only be called inside `hbase-clj.driver/with-table`
   --------------------
   usage: 
   (put! [id, attrs] [id, attrs] ....)
   attrs should be a hashmap of {family-> {col-> val}}"
  [& records]
  (validate-tables)
  (.put *table* 
        (vec 
          (for [[id attr-map] records] 
            (get-putter id attr-map)))))

(defn get
  "Get from HBase Table according to specified ids
   Should only be called inside `hbase-clj.driver/with-table`
   --------------------
   usage:
   (get id|attrs id|attrs ....)
   (get :with-versions ...)

   id|attrs could be id or a pair like vec: [id attrs]
   which attrs should be a hash-map of {family-> col/cols}
   col/cols could either be a col, a vector of cols or :* which stands for all cols
   --------------------
   e.g: 
       (get \"001\" 
            [\"002\" {:info :*}]
            [\"003\" {:info [:age :name] :follow :*}]
           ....)
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

(defn scan 
  "Scan inside HBase Table according to certain rules
   Should only be called inside `hbase-clj.driver/with-table`
   --------------------
   usage:
   (scan & options)
   (scan :with-versions & options)
   options could contain these keys:
     - :eager?       If set to true, return a hash-map similar to the result of `get`, if false, returns a lazy scanner to fetch results later.
     - :start-id     The ID the htable-scan starts from (including)
     - :stop-id      The ID the htable-scan stops at (non-including)
     - :cache-size   The number of rows fetched when getting a \"next\" item, only takes effect when `eager?` is set to false
     - :small?       Determine if this is a \"small\" scan, see: https://hbase.apache.org/0.94/apidocs/org/apache/hadoop/hbase/client/Scan.html#setSmall(boolean)
     - :max-versions The max version to fetch on each record
     - :time-range   The time-range to fetch on each record
     - :attrs        Same as the definition of `<attrs>` in `(get ..)`"
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

    (doseq [[f c] attrs]
      (if (= c :*) 
        (.addFamily scanner 
                    (encode :keyword f))
        (if (coll? c)
          (doseq [c c] (addcolumn f c))
          (addcolumn f c))))

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
                            (get-res)) 
                      (do (.close res) nil))))]
          (get-res))))))

(defn incr!
  "Make atomic incrementations on certain row-family-col-s inside an HBase Table
   Should only be called inside `hbase-clj.driver/with-table`
   --------------------
   usage: 
   (incr! [id, attrs] [id, attrs] ....)
   attrs should be a hash-map of {family-> {col-> val}}"

  [& rows]
  (validate-tables)
  (let [{:keys [id-type families]} *schema*]
    (.batch *table*
            (vec 
              (map 
                (fn [[id attr-map]]
                  (get-increr id attr-map)) 
                rows)))))

;;The delete function fails with java.lang.UnsupportedOperationException
;;TODO: Figure out why
#_(defn delete!
  "Delete from HBase Table
   Should only be called inside `hbase-clj.driver/with-table`
   --------------------
   usage: 
   (delete! [id, attrs] [id, attrs] ....)
   attrs should be a hash-map of of {family-> col}, drops the whole family when col set to :*
   e.g: [[:info [:age :name]] :follow]"
  [& rows]
  (validate-tables)
  (.delete *table*
           (vec 
             (for [r rows] 
               (if (coll? r) 
                 (get-deleter (first r) (or (second r) []))
                 (get-deleter r []))))))


;;----------------------------HIDDEN FUNCTIONS--------------------------------


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
                (encode (or (typemap k) (:--vtype typemap)) v)))))

    putter))

(defn- get-getter 
  [id attrs {:keys [max-versions time-range]}]
  (let [{:keys [id-type families]} *schema*
        ^Get getter (Get. (encode id-type id))
        addcolumn (fn [f c]
                    (.addColumn 
                      getter 
                      (encode :keyword f) 
                      (encode (get-in families [f :--ktype]) c)))]
    (if max-versions 
      (.setMaxVersions getter max-versions))
    (if time-range
      (.setTimeRange getter 
                     (long (first time-range)) 
                     (long (second time-range))))
    (doseq [[f c] attrs]
      (if (= c :*)
        (.addFamily getter 
                    (encode :keyword f))
        (if (coll? c)
          (doseq [c c] (addcolumn f c))
          (addcolumn f c))))

    getter))

(defn- get-increr
  [id attrs]
  (let [{:keys [id-type families]} *schema*
        ^Increment increr (Increment. (encode id-type id))
        addcolumn (fn [f c v]
                    (.addColumn 
                      increr 
                      (encode :keyword f) 
                      (encode (get-in families [f :--ktype])
                              c)
                      (long v)))]

    (doseq [[f attrs] attrs]
      (doseq [[c v] attrs]
        (addcolumn f c v)))

    increr))

(defn- get-deleter 
  [id attrs]
  (let [{:keys [id-type families]} *schema*
        ^Delete deleter (Delete. (encode id-type id))
        addcolumn (fn [f c]
                    (.deleteColumn 
                      deleter 
                      (encode :keyword f) 
                      (encode (get-in families [f :--ktype]) c)))]

    (doseq [[f c] attrs]
      (if (= c :*)
        (.deleteFamily deleter (encode :keyword f))
        (if (coll? c)
          (doseq [c c] (addcolumn f c))
          (addcolumn f c))))

    deleter))
