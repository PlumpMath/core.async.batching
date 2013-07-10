(ns core.async-batching
  (require [clojure.core.async :as async :refer :all]
           [core.scheduler :as sched]))

(def first-ticks [{:ticker "AAPL" :price 41000}
                  {:ticker "GOOG" :price 62000}])

(defn rand-dir []
  (condp = (int (rand 3))
    0 :down
    1 :flat
    2 :up))

(defn next-tick [last-tick]
  (let [dir (rand-dir)
        price (:price last-tick)]
    (assoc last-tick
      :dir dir
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



(defn start-feed []
  (let [tickfn (seqfn (tickseq first-ticks))]
    (sched/make-scheduler #(println (tickfn)) 0 500)))

;; (def feed (start-feed))
;; (sched/shutdown feed)



(def tickfeed (chan 1000))
