(ns merlion.core
  (:require [merlion.etcd :as etcd]
            [clojure.pprint :refer [pprint]]
            [manifold.stream :as s]
            [manifold.deferred :as d]
            [clojure.string :as str]
            [clojure.set :as set]
            [clojure.instant]
            [clojure.core.async :as async
             :refer [<! go chan >! close! alts!]]
            [aleph.http :as http])
  (:gen-class))

(defn spy [a] (pprint a) a)

(defn proxy-handler1 [backends-ch req]
  (let [ch (chan)]
    (async/>!! backends-ch [req ch])
    (s/take! (s/->source ch))))

(defn proxy-handler [ch req]
  (proxy-handler1 ch req))

(defn parse-address [a]
  (let [[address port] (str/split a #":")]
    {:address address :port (Integer/parseInt port)}))

(defn make-backend-channel [backend]
  (let [ch (chan)
        backend-addr (parse-address (:listen-address backend))]
    (go
      (loop []
        (when-let [[req out-ch] (<! ch)]
          (>! out-ch
              (http/request (assoc req
                                   :raw-stream? true
                                   :follow-redirects? false
                                   :server-name (:address backend-addr)
                                   :server-port (:port  backend-addr))))
          (close! out-ch)
          (recur)))
      (println [:shutdown backend]))
    ch))

(defn s->millepoch-time [s]
  (.getTimeInMillis (clojure.instant/read-instant-calendar s)))

(defn millepoch-time-now []
  (.getTime (java.util.Date.)))

(defn healthy-backend?
  "true if backend seen more recently than time"
  [time b]
  (> (:last-seen-at-millepoch b) time))

(defn combined-config-watcher [prefix]
  (let [config-watcher (etcd/watch-prefix prefix)
        out-ch (chan)]
    (go
      (loop [backend-prefix nil
             backend-watcher nil]
        (let [config (etcd/get-prefix prefix)
              new-bp (:upstream-service-etcd-prefix config)
              backend-watcher
              (if (and backend-watcher (= new-bp backend-prefix))
                backend-watcher
                (and new-bp (etcd/watch-prefix new-bp)))]
          (>! out-ch {:config config
                      :backends (if new-bp (etcd/get-prefix new-bp) {})})
          (alts! (remove nil? [config-watcher backend-watcher]))
          (recur new-bp backend-watcher))))
    out-ch))

(defn update-backends-state [app-state backends-spec]
  (let [before (reduce (fn [m bk]
                         (assoc m (:listen-address bk) bk))
                       {}
                       (:backends app-state))
        after  (reduce (fn [m bk]
                         (assoc m (:listen-address bk) bk))
                       {}
                       (vals backends-spec))
        b-addrs (set (keys before))
        a-addrs (set (keys after))
        deleted (set/difference b-addrs a-addrs)
        create (set/difference a-addrs b-addrs)]
    (pprint [:update-backends before after deleted create])
    (dorun (map (fn [b] (close! (:channel (get before b)))) deleted))
    (map (fn [a]
           (let [bk (get after a)]
             (assoc bk
                    :channel
                    (or (:channel (get before a))
                        (make-backend-channel bk))
                    :last-seen-at-millepoch
                    (s->millepoch-time (:last-seen-at bk))
                    )))
         a-addrs)))

(defn open-listener [addr dispatch-ch]
  (http/start-server (partial proxy-handler dispatch-ch)
                     (parse-address addr)))

(defn update-app-state [app-state m]
  (println [:update-app-state app-state m])
  (let [dispatch-ch (:dispatch-channel app-state)
        config (:config m)
        backends-spec (:backends m)
        addr (:listen-address config)]
    (assoc app-state
           :upstream-freshness-ms
           (* 1000 (Integer/parseInt (:upstream-freshness config)))
           :dispatch-channel dispatch-ch
           :listen-address (:listen-address config)
           :listener
           (if-let [l (:listener app-state)]
             (if (= (:listen-address app-state) addr)
               l
               (do
                 (.close l)
                 (open-listener addr dispatch-ch)))
             (open-listener addr dispatch-ch))
           :backends (update-backends-state app-state backends-spec))))

(defn run-server [prefix]
  (let [config-ch (combined-config-watcher prefix)
        shutdown-ch (chan)
        dispatcher-ch (chan)]
    (go
      (loop [app-state {:dispatch-channel dispatcher-ch}]
        (let [backends (:backends app-state)
              [val ch] (alts! [config-ch dispatcher-ch shutdown-ch])]
          (condp = ch
            config-ch
            (recur (spy (update-app-state app-state val)))
            dispatcher-ch
            (let [since (- (millepoch-time-now)
                           (:upstream-freshness-ms app-state))
                  good-backends (filter (partial healthy-backend? since)
                                        backends)]
              (if (seq good-backends)
                (alts! (map (fn [b] [(:channel b) val]) good-backends))
                (>! (second val) {:status 500 :headers {}
                                  :body "No healthy downstreams"}))
              (recur app-state))
            shutdown-ch
            (do
              (dorun (map (fn [b] (async/close! (:channel b))) backends))
              (when-let [l (:listener app-state)]
                (.close l))
              (println "stopping"))))))
    shutdown-ch))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!")
  (let [s (run-server (first args))]
    (Thread/sleep (* (Integer/parseInt (second args)) 1000))
    (async/put! s :done)))
