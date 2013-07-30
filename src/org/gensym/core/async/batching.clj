(ns org.gensym.core.async.batching
  (:require [clojure.core.async.impl.protocols :as impl])
  (:import [java.util LinkedList]))

(deftype BatchingBuffer [items]
  impl/Buffer

  (full? [this]
    false)

  (remove! [this]
    (dosync
     (let [ret @items]
       (ref-set items [])
       ret)))

  (add! [this itm]
    (dosync (alter items conj itm)))

  clojure.lang.Counted
  (count [this]
    (if (empty? @items)
      0
      1)))

(defn batching-buffer []
  (BatchingBuffer. (ref [])))


