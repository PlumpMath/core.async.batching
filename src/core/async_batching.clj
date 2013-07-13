(ns core.async-batching
  (:require [clojure.core.async :as async :refer :all]
           [clojure.core.async.impl.protocols :as impl]
           [core.scheduler :as sched])
  (:import [java.util LinkedList]))

(def first-ticks [{:ticker "AAPL" :price 41000 :seq 0}
                  {:ticker "GOOG" :price 62000 :seq 0}])

(defn rand-dir []
  (condp = (int (rand 3))
    0 :down
    1 :flat
    2 :up))

(defn next-tick [last-tick]
  (let [dir (rand-dir)
        price (:price last-tick)
        seq (inc (:seq last-tick))]
    (assoc last-tick
      :dir dir
      :seq seq
      :price (condp = dir
               :down (- price 1)
               :flat price
               :up (+ price 1)))))

(defn tickseq [init-ticks]
  (->>  (iterate #(map next-tick %) init-ticks)
        (map (fn [ticks]
               (map (fn [tick] (dissoc tick :dir))
                    (filter (fn [{dir :dir}]
                              (not (= :flat dir))) ticks))))
        (filter #(not (empty? %)))))

(defn seqfn [s]
  (let [a (atom s)]
    (fn []
      (let [v (first @a)]
        (swap! a #(rest %))
        v))))

(defn start-feed [publish-fn freq]
  (let [tickfn (seqfn (tickseq first-ticks))]
    (sched/make-scheduler #(publish-fn (tickfn)) 0 freq)))

;;(def feed (start-feed println))
;;(sched/shutdown feed)

(def tick-channel (chan 1000))

(defn start-feed-publisher [freq]
  (start-feed #(go (>! tick-channel %)) freq))

(defn start-feed-subscriber [display-fn freq]
  (sched/make-scheduler #(display-fn (<!! (go (<! tick-channel)))) 0 freq))

;; =============================================

;; Examples

;;(def publisher (start-feed-publisher 100))


;; =============================================

;; No batching - the publisher is is writing at 10X the rate the
;; subscriber is reading -> backed up subscriber

;;(def subscriber (start-feed-subscriber println 1000))

;; =============================================

;; First, let's create a channel where we put collections of all the
;; messages that have been published so far.

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


(def batch-tick-channel (chan (batching-buffer)))

(defn start-batch-feed-subscriber [display-fn freq]
  (sched/make-scheduler
   (fn []
     (println "running")
     (display-fn (<!! (go (<! batch-tick-channel))))) 0 freq))



(defn start-batch-feed-publisher [freq]
  (start-feed #(go (>! batch-tick-channel %)) freq))

(def publisher (start-batch-feed-publisher 100))
(def batch-subscriber (start-batch-feed-subscriber println 1000))
