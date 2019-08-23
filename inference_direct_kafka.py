import cv2
import sys
import time
import heapq
import numpy as np
import tensorflow as tf

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

pb_path = "/Users/ouyuefeng/workspace-spark-Kafka/project/opt_frozen_mobilenet.pb"
labels_path = "/Users/ouyuefeng/workspace-spark-Kafka/project/labels.txt"
save_path = "file:///Users/ouyuefeng/workspace-spark-Kafka/project/online_inference@"

def image_formater(img_buffer):
    buffer = np.frombuffer(img_buffer, dtype=np.uint8)
    shape = buffer[:4]
    pixels = buffer[4:]
    height = shape[0]*256 + shape[1]
    width = shape[2]*256 + shape[3]
    img = np.reshape(pixels, (height, width, 3))
    return img

def cv2image(img):
    image_data = img[:,:,::-1] / 255.0  # IMPORTANT!!!!!!!!! Color channels BGR -> RGB
    image_data = cv2.resize(image_data, (224, 224))
    image_data = image_data[None, :] #（1，224，224，3）
    return image_data

# the images parameter is a RDD: (numpy_arrays)
def inference(images, model, labels):
    result = []
    tf.import_graph_def(graph_def, name='')
    for image_data in images:
        with tf.Session() as session:
            # Get output and feed input
            softmax_tensor = session.graph.get_tensor_by_name('MobilenetV2/Predictions/Reshape_1:0')
            predictions = session.run(softmax_tensor,{'input:0': image_data})
            # Show the results
            max_num_index_list = map(predictions[0].tolist().index, heapq.nlargest(5, predictions[0]))
            top_list = list(max_num_index_list)
            # label_name + percentage
            predict_with_score = []
            for idx in range(5):
                predict_label = labels[top_list[idx]]
                score = predictions[0][top_list[idx]]
                predict_with_score.append((predict_label, score))
            result.append(predict_with_score)
    return result

def processDstream(rdd, graph_def_bd, labels_bd):
    if not rdd.isEmpty():
        print('Batch is not empty!')
        print('Proceeding inference...')

        t = time.strftime("%Y/%m/%d_%H.%M.%S", time.localtime())

        # Process each image
        images = rdd.map(lambda img: image_formater(img)) \
                    .map(lambda img: cv2image(img))

        # tf predict
        tf_inference = images.mapPartitions(lambda imgs: inference(imgs, graph_def_bd.value, labels_bd.value))

        # saveAsResult
        tf_inference.map(lambda ps: "|".join(["%s:%f" % (l, s) for (l, s) in ps])).repartition(4) \
            .saveAsTextFile(save_path + t)

        print('Inference done. The result is saved at: ' + save_path+ t)


if __name__ == '__main__':

    spark = SparkSession.builder \
        .appName("KafkaStreamingInference") \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('WARN')
    ssc = StreamingContext(sc, 10)

    brokers, topic = sys.argv[1:]
    kafkaParams = {"metadata.broker.list": brokers}

    # load tf model and broadcast
    with tf.gfile.FastGFile(pb_path,'rb') as f:
        graph_def = tf.GraphDef()
        graph_def.ParseFromString(f.read())
    graph_def_bd = sc.broadcast(graph_def)

    # label_mapping
    labels = []
    with open(labels_path, 'r') as fl:
        for line in fl.readlines():
            label = line.split(':')[1]
            labels.append(label[4:-1])
    labels_bd = sc.broadcast(labels)

    # The default utf-8 decoder can not decode the streaming bytes
    # So do nothing to the bytes while polling the stream
    imageBytes_KVpair = KafkaUtils.createDirectStream(ssc, [topic],     \
                        kafkaParams,                                    \
                        keyDecoder=lambda x: x,                         \
                        valueDecoder=lambda x: x)

    imageBytes = imageBytes_KVpair.map(lambda x: x[1])
    imageBytes.count().pprint()
    imageBytes.foreachRDD(lambda rdd: processDstream(rdd, graph_def_bd, labels_bd))

    ssc.start()
    ssc.awaitTermination()
