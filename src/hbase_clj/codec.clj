(ns hbase-clj
  (:import 
    (org.apache.hadoop.hbase.util Bytes)))

(defmulti encode (fn [t _] t))
(defmulti decode (fn [t _] t))

(defmacro defcodec 
  [t & {:keys [encoder decoder]}]
  `(do 
     (defmethod encode ~t [t# data#] (~encoder data#))
     (defmethod decode ~t [t# data#] (~decoder data#))))

(defcodec :int 
  :encoder (fn [data] (Bytes/toBytes (int data))) 
  :decoder Bytes/toInt)

(defcodec :long 
  :encoder (fn [data] (Bytes/toBytes (long data)))
  :decoder Bytes/toLong)

(defcodec :string 
  :encoder (fn [data] (Bytes/toBytes data))
  :decoder Bytes/toString)

(defcodec :keyword 
  :encoder (fn [data] (Bytes/toBytes (name data)))
  :decoder (fn [data] (keyword (Bytes/toString data))))

(defcodec :raw 
  :encoder (fn [data] (Bytes/toBytes data))
  :decoder identity)
