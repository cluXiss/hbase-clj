(ns hbase-clj.codec
  (:require 
    [hbase-clj.byte-utils :as b]))

(defmulti encode (fn [t _] t))
(defmulti decode (fn [t _] t))

(defmacro defcodec 
  "Registers a new codec transformation to use in `hbase-clj.core/def-htable`.
   -----------------------------------
   e.g:
   (defcodec :long
     :encoder (fn [data] (Bytes/toBytes (long data)))
     :decoder Bytes/toLong)"
  [t & {:keys [encoder decoder]}]
  `(do 
     (defmethod encode ~t [t# data#] (~encoder data#))
     (defmethod decode ~t [t# data#] (~decoder data#))))

(defcodec :long 
  :encoder (fn [data] (b/->bytes (long data)))
  :decoder b/->long)

(defcodec :string 
  :encoder (fn [data] (b/->bytes data))
  :decoder b/->str)

(defcodec :keyword 
  :encoder (fn [data] (b/->bytes (name data)))
  :decoder (fn [data] (keyword (b/->str data))))

(defcodec :raw 
  :encoder identity
  :decoder identity)
