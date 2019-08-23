import os
import sys
import cv2
import math
import numpy as np
from confluent_kafka import Producer

def delivery_report(err, msg):
    """
    Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush().
    """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

if __name__ == '__main__':
    if(len(sys.argv) == 3):

        topic = sys.argv[1]
        img_path = sys.argv[2]
        p = Producer({'bootstrap.servers': 'localhost:9092'})

        # Open file
        image_files = os.listdir(img_path)
        print('publishing images in directory:' + img_path)

        for image in image_files:
            # Trigger any available delivery report callbacks from previous produce() calls
            p.poll(0)

            # convert jpg to str
            print('Sending image file:', image)

            img = cv2.imread(img_path+'/'+image)

            height = img.shape[0]
            width = img.shape[1]

            # the maximum message size a Kafka server can receive is 1000000
            # so the number of pixels should not exceed 333333 (for 3 channels)
            # Besides, if the further DNN inference require fix-scale input
            # we can do resize here to match the input size
            if height*width > 333333:
                ratio = math.sqrt(height*width/333333)
                height = int(height / ratio)
                width = int(width / ratio)
                img = cv2.resize(img, (width, height))

            # Add the shape information of image to the stream
            shape2bytes = np.array([height//256, height%256, width//256, width%256], dtype=np.uint8)
            shape2bytes = bytes(shape2bytes)

            imgBytes = bytes(img)
            pack = shape2bytes+imgBytes
            print(len(pack))

            # Asynchronously produce a message, the delivery report callback
            # will be triggered from poll() above, or flush() below, when the message has
            # been successfully delivered or failed permanently.
            p.produce(topic, pack, callback=delivery_report)

        # Wait for any outstanding messages to be delivered and delivery report
        # callbacks to be triggered.
        p.flush()
        print('publish complete')

    else:
        print("Bad argvs!")
        print("Usage: python confluent_kafka_prodecer.py topic image_path")
