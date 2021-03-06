(ns hbase-clj.byte-utils
  (:refer-clojure 
    :exclude [concat repeat assoc assoc! take take-last contains? find])
  (:import 
    (org.cluxis.hbase_clj.helper ByteConversion)
    (org.apache.hadoop.hbase.util Bytes)))

(defn size-of [t]
  (case t
    :bool (Bytes/SIZEOF_BOOLEAN)
    :byte (Bytes/SIZEOF_BYTE)
    :char (Bytes/SIZEOF_CHAR)
    :double (Bytes/SIZEOF_DOUBLE)
    :float (Bytes/SIZEOF_FLOAT)
    :int (Bytes/SIZEOF_INT)
    :long (Bytes/SIZEOF_LONG)
    :short (Bytes/SIZEOF_SHORT)))

(defn long-to-bytes [^long x]
  (Bytes/toBytes x))

(defn ->bytes 
  "Coerce to bytes"
  [x]
  ;; need to coerce explicitly, see: http://stackoverflow.com/questions/12586881/clojure-overloaded-method-resolution-for-longs
  (condp = (class x)
    java.lang.Long (ByteConversion/fromLong (Long. x))  
    java.lang.Integer (Bytes/toBytes (Integer. x))  
    java.lang.Short (Bytes/toBytes (Short. x))  
    (Bytes/toBytes x)))

(defn concat 
  "concat several byte arraies together"
  [& b]
  (Bytes/add (into-array b)))

(defn ->bcat
  "Coerce to bytes and concat"
  [& b]
  (apply concat (map ->bytes b)))

(defn repeat 
  "Repeat a byte[] several times into a new byte[]"
  [n b]
  (Bytes/multiple b n))

(defn ->brepeat 
  "Coerce to bytes and repeat"
  [n b]
  (repeat n (Bytes/toBytes b)))

(defn padhead 
  "prepend zeroes to the head of a byte[]"
  [n b]
  (Bytes/padHead b n))

(defn padtail 
  "append zeroes to the tail of a byte[]"
  [n b]
  (Bytes/padTail b n))

(defn ->bpadhead 
  "Coerce to bytes and padhead"
  [n b]
  (Bytes/padHead (Bytes/toBytes b) n))

(defn ->bpadtail 
  "Coerce to bytes and padtail"
  [n b]
  (Bytes/padTail (Bytes/toBytes b) n))

(defn assoc! 
  [b offset x]
  (Bytes/putByte b offset x))

(defn assoc 
  [b offset x]
  (Bytes/putByte (Bytes/copy b) offset x))

(defn insert! 
  [tgt offset src 
   src-offset src-len]
  (Bytes/putBytes tgt offset src src-offset src-len))

(defn insert
  [tgt offset src 
   src-offset src-len]
  (Bytes/putBytes (Bytes/copy tgt) offset src src-offset src-len))

(defn take 
  "take the first n bytes from a byte[], 
   returns a new byte[]"
  [n b]
  (Bytes/head b n))

(defn take-last 
  "take the last n bytes from a byte[], 
   returns a new byte[]"
  [n b]
  (Bytes/tail b n))

(defn ->btake 
  "Coerce to bytes and take"
  [n b]
  (take n (Bytes/toBytes b)))

(defn -btake-last 
  "Coerce to bytes and take-last"
  [n b]
  (take-last n (Bytes/toBytes b)))

(defn len 
  "Get the length of an byte[]"
  [b]
  (Bytes/len b))

(defn ->blen 
  "Coerce to bytes and get length"
  [b]
  (len (Bytes/toBytes b)))

(defn contains? 
  "Returns true if a byte[] contains a specified byte or byte[]"
  [b t]
  (Bytes/contains b t))

(defn find 
  "Returns the index of a specified byte or byte[] inside a byte[]"
  [b t]
  (Bytes/indexOf b t))

(defn ->str
  ([b]
   (Bytes/toString b))
  ([b offset]
   (Bytes/toString b offset))
  ([b offset len]
   (Bytes/toString b offset len)))

(defn ->bool [b]
  (Bytes/toBoolean b))

(defn ->short 
  ([b]
   (Bytes/toShort b))
  ([b offset]
   (Bytes/toShort b offset))
  ([b offset len]
   (Bytes/toShort b offset len)))

(defn ->int 
  ([b]
   (Bytes/toInt b))
  ([b offset]
   (Bytes/toInt b offset))
  ([b offset len]
   (Bytes/toInt b offset len)))

(defn ->long 
  ([b]
   (Bytes/toLong b))
  ([b offset]
   (Bytes/toLong b offset))
  ([b offset len]
   (Bytes/toLong b offset len)))
