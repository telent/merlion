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

(defn refresh-backend-watcher [watcher old-prefix new-prefix]
  (if (and watcher (= old-prefix new-prefix))
    watcher
    (do (if watcher (close! watcher))
        (etcd/watch-prefix new-prefix))))
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
          (println [:got-backend-req
                    backend-addr])
          (>! out-ch
              (http/request (assoc req
                                   :raw-stream? true
                                   :follow-redirects? false
                                   :server-name (:address backend-addr)
                                   :server-port (:port  backend-addr))))
          (close! out-ch)
          (println "done")
          (recur )))
      (println [:shutdown backend]))
    ch))

(defn update-listeners-from-config [ch current new-address]
  ;; open server sockets for all the addresses in config that aren't
  ;; in listeners; drain and close all the sockets in
  ;; listeners which aren't in config.
  ;; we only have one listener right now, so this takes
  ;; some shortcuts
  (if new-address
    (if (get current new-address)
      current
      (do
        ;; need to check this isn't breaking in-flight requests
        (when-let [old (first (vals current))]
          (println [:closing old])
          (.close old))
        {new-address
         (http/start-server (partial proxy-handler ch)
                            (parse-address  new-address))}))
    current))

(defn update-backends-from-config [current new-backends]
  (let [before (set (keys current))
        after (set (keys new-backends))
        deleted (set/difference before after)
        create (set/difference after before)]
    (dorun (map (fn [b] (close! (:channel (get current b)))) deleted))
    (reduce-kv (fn [m k v]
                 (assoc m k
                        (if (:channel v)
                          v
                          (assoc v :channel
                                 (make-backend-channel v)))))
               {}
               (apply dissoc new-backends deleted))))

(defn print-state [l b]
  (pprint {:listeners l :backends b}))

(defn s->millepoch-time [s]
  (.getTimeInMillis (clojure.instant/read-instant-calendar s)))

(defn millepoch-time-now []
  (.getTime (java.util.Date.)))

(defn healthy-backend?
  "true if backend seen more recently than time"
  [time b]
  (> (s->millepoch-time (:last-seen-at b))
     time))

(defn run-server [prefix]
  (let [shutdown-ch (chan)
        config-watcher (etcd/watch-prefix prefix)
        backends-ch (chan)]
    (go
      (loop [listeners {}
             backends {}
             backend-prefix nil
             backend-watcher nil]
        (let [config (etcd/get-prefix prefix)
              new-bp (:upstream-service-etcd-prefix config)
              backends (if new-bp
                         (update-backends-from-config
                          backends (etcd/get-prefix new-bp))
                         backends)
              listeners (update-listeners-from-config
                         backends-ch
                         listeners
                         (:listen-address config))]
          (print-state listeners backends)
          (let [[value ch]
                (alts! (remove nil?
                               [config-watcher
                                backend-watcher
                                shutdown-ch
                                backends-ch]))]
            (when (= ch backends-ch)
              (let [since (- (millepoch-time-now)
                             (* 1000 (Integer/parseInt (:upstream-freshness config))))
                    good-backends (filter (partial healthy-backend? since)
                                          (vals backends))]
                (if (seq good-backends)
                  (alts! (map (fn [b] [(:channel b) value]) good-backends))
                  (>! (second value) {:status 500 :headers {}
                                      :body "No healthy downstreams"}))))
            (if (= ch shutdown-ch)
              (.close (first (vals listeners)))
              (recur listeners
                     backends
                     new-bp
                     (refresh-backend-watcher backend-watcher backend-prefix new-bp))))))
      (println "quit"))
    shutdown-ch))





(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!")
;  (add-watch etcd/configuration :update-frontend update-frontend)
;  (etcd/watch-config (first args))
  (println "running"))
