(ns clj-kinesis.core
  (:use [alex-and-georges.debug-repl])
  (:import [java.util UUID])
  (:import [com.amazonaws.services.kinesis.clientlibrary.interfaces.v2 IRecordProcessor IRecordProcessorFactory]
           [com.amazonaws.auth DefaultAWSCredentialsProviderChain]
           [com.amazonaws.services.kinesis.clientlibrary.lib.worker Worker Worker$Builder KinesisClientLibConfiguration]))

(defn- record-processing-fn [deserialise-fn msg-processing-fn]
  (fn [record]
    ; what if deserialise or msg-processing fails???
    (-> record
        (wrapping-exceptions decode)
        (wrapping-exceptions deserialise-fn)
        (wrapping-exceptions msg-processing-fn))))

(defn- record-processor [processing-fn]
  (reify IRecordProcessor
    (initialize [this initializationInput])
    (processRecords [this processRecordsInput]
      (let [records (.getRecords processRecordsInput)]
        (map processing-fn records)))
    (shutdown [this shutdownInput])))

(defn- record-processor-factory [processing-fn]
  (reify IRecordProcessorFactory
    (createProcessor [this] (record-processor processing-fn))))

(defn- not-exception [in]
  (and (not (nil? (ex-data in))) (not (nil? in))))

(defn- wrapping-exceptions [input function]
  (if-not (ex-data input)
    (try
      (function input)
      (catch Exception e
        (ex-info (.getMessage e) {} e)))))

(defn decode [in]
  (-> in
      ((fn [input] (.getData input)))
      ((fn [input] (.array input)))))



(defn- client-config [application-name stream-name worker-id region]
  (->
    (KinesisClientLibConfiguration. application-name stream-name (DefaultAWSCredentialsProviderChain.) worker-id)
    (.withRegionName region)))

;; options for shard iterator??


(defn consume [application-name stream-name msg-processing-fn & {:keys [worker-id deserialise-fn region-name]
                                                                 :or {worker-id (str (UUID/randomUUID))
                                                                      deserialise-fn identity
                                                                      region-name "eu-west-1"}}]

  (-> (Worker$Builder.)
      (.recordProcessorFactory (record-processor-factory (record-processing-fn deserialise-fn msg-processing-fn)))
      (.config (client-config application-name stream-name worker-id region-name))
      (.build)
      (.run)))
