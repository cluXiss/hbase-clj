# A Quick Tutorial to hbase-clj

First, require the required namespaces

```
(require 
 '(hbase-clj
	 [core :refer :all]
	 [manage :refer :all])))
```

We could define an hbase handler via [hbase-clj.core/defhbase](http://cluxiss.github.io/hbase-clj/hbase-clj.core.html#var-defhbase) 
The code below defines a handler which connects to the localhost HBase
```
(defhbase test-hbase 
  "hbase.zookeeper.quorum" "localhost")
```


We could create a table via [hbase-clj.manage/create-table!](http://cluxiss.github.io/hbase-clj/hbase-clj.manage.html#var-create-table.21)
The code below creates a table named `test_stu` using the hbase handler defined above.
```
(def table-name "test_stu")

(create-table! 
  test-hbase table-name
  "info" "score")
```

Before we perform any CRUDs on the table, we must create a "schema" on it.
The Schema describes how the data from an HBaseTable could be transformed into clojure data-structures.
We define a schema via [hbase-clj.core/def-hbtable](http://cluxiss.github.io/hbase-clj/hbase-clj.core.html#var-def-hbtable)
```
(def-hbtable test-table
  {:id-type :string 
   :table-name table-name
   :hbase test-hbase}
  :info  {:--ktype :keyword  :--vtype :long :name :string}
  :score {:--ktype :keyword  :--vtype :long})
```

Then we could perform CRUDs inside a table using [hbase-clj.core/with-table](http://cluxiss.github.io/hbase-clj/hbase-clj.core.html#var-with-table)
```
(with-table test-table
  (put! 
    ["101" {:info {:age 17 :name "Alice"}
            :score {:math 99 :physics 78}}]

    ["102" {:info {:age 19 :name "Bob"}
            :score {:math 63 :physics 52}}]))
```

The [hbase-clj.core/get](http://cluxiss.github.io/hbase-clj/hbase-clj.core.html#var-get) function gets data according to row-keys and other constraints.
and returns it as a vector of `vec: [row-key data]`
```
(get "102") 
;; => [["102"
        {:info {:age 19 :name "Bob"}
         :score {:math 63 :physics 52}}]]
```

We can specify the families and cols either
```
(get "102" {:info :*}) 
;; => [["102" {:info {:age 19 :name "Bob"}}]]

(get "102" {:info :age}) 
;; => [["102" {:info {:age 19}}]]

(get "102" {:info [:age :name]}) 
;; => [["102" {:info {:age 19 :name "Bob"}}]]
```

If specified `with-versions` as the first argument, get acts a little different
```
(get :with-versions "102" {:info :age})
;; => [["102" {:info {:age [ {:val 19 :timestamp xxxxxxx} 
   	                         ...]}}]]
```

And we've got other tasty functions like 
* [hbase-clj.core/scan](http://cluxiss.github.io/hbase-clj/hbase-clj.core.html#var-scan)
* [hbase-clj.core/incr!](http://cluxiss.github.io/hbase-clj/hbase-clj.core.html#var-incr)
* ...

Check the [complete doc](http://cluxiss.github.io/hbase-clj/)!
