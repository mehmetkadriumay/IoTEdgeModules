# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for
# full license information.
import time
import os
import sys
import asyncio
#from six.moves import input
import threading
from azure.iot.device.aio import IoTHubModuleClient
import json
#from confluent_kafka import Consumer
from aiokafka import AIOKafkaConsumer


# global counters
TEMPERATURE_THRESHOLD = 25
TWIN_CALLBACKS = 0
RECEIVED_MESSAGES = 0
KAFKA_BOOTSTRAP_SERVER = "kafka:9092"
KAFKA_TOPIC = "OSDU_Topic"

async def main():
    try:
        print ( "IoT Hub Client for Python" )

        # The client object is used to interact with your Azure IoT hub.
        module_client = IoTHubModuleClient.create_from_edge_environment()

        # connect the client.
        await module_client.connect()

        # Define behavior for receiving an input message on input1
        # Because this is a filter module, we forward this message to the "output1" queue.
        
        consumer = AIOKafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                group_id="OSDU_Group"
            )
        
        await consumer.start()

        async def kafka_listener(consumer):
            global KAFKA_BOOTSTRAP_SERVER
            global KAFKA_TOPIC

            print("Connecting to new server: ", KAFKA_BOOTSTRAP_SERVER)
            

            try:
                async for msg in consumer:
                    print("consuned: ", msg.topic, msg.value, msg.timestamp)
            except Exception as e:
                print ("Connection error %s " % e )

        '''
        async def input1_listener(module_client):
            global RECEIVED_MESSAGES
            global TEMPERATURE_THRESHOLD

            while True:
                try:
                    input_message = await module_client.receive_message_on_input("input1")  # blocking call
                    #input_message = await module_client.on_message_received("input1")  # blocking call
                    print("Input Message is")
                    message = input_message.data
                    size = len(message)
                    message_text = message.decode('utf-8')
                    print ( "    Data: <<<%s>>> & Size=%d" % (message_text, size) )
                    custom_properties = input_message.custom_properties
                    print ( "    Properties: %s" % custom_properties )
                    RECEIVED_MESSAGES += 1
                    print ( "    Total messages received: %d" % RECEIVED_MESSAGES )
                    data = json.loads(message_text)
                    if "machine" in data and "temperature" in data["machine"] and data["machine"]["temperature"] > TEMPERATURE_THRESHOLD:
                        custom_properties["MessageType"] = "Alert"
                        print ( "Machine temperature %s exceeds threshold %s" % (data["machine"]["temperature"], TEMPERATURE_THRESHOLD))
                        await module_client.send_message_to_output(input_message, "output1")
                except Exception as ex:
                    print ( "Unexpected error in input1_listener: %s" % ex )

        '''
        # twin_patch_listener is invoked when the module twin's desired properties are updated.
        async def twin_patch_listener(module_client):
            global TWIN_CALLBACKS
            global TEMPERATURE_THRESHOLD
            global KAFKA_BOOTSTRAP_SERVER
            global KAFKA_TOPIC

            while True:
                try:
                    data = await module_client.receive_twin_desired_properties_patch()  # blocking call
                    print( "The data in the desired properties patch was: %s" % data)
                    if "TemperatureThreshold" in data:
                        TEMPERATURE_THRESHOLD = data["TemperatureThreshold"]
                        TWIN_CALLBACKS += 1
                    
                    if "KAFKA_BOOTSTRAP_SERVER" in data:
                        KAFKA_BOOTSTRAP_SERVER = data["KAFKA_BOOTSTRAP_SERVER"]
                        TWIN_CALLBACKS += 1

                    if "KAFKA_TOPIC" in data:
                        KAFKA_TOPIC = data["KAFKA_TOPIC"]
                        TWIN_CALLBACKS += 1
                    
                    print ( "Total calls confirmed: %d\n" % TWIN_CALLBACKS )


                except Exception as ex:
                    print ( "Unexpected error in twin_patch_listener: %s" % ex )

        
        # define behavior for halting the application
        def stdin_listener():
            while True:
                try:
                    selection = input("Press Q to quit\n")
                    if selection == "Q" or selection == "q":
                        print("Quitting...")
                        break
                except:
                    time.sleep(10)
        
        # Schedule task for C2D Listener
        listeners = asyncio.gather(kafka_listener(consumer), twin_patch_listener(module_client))
        #listeners = asyncio.gather(input1_listener(module_client), twin_patch_listener(module_client))
        print ( "The sample is now waiting for messages. ")

        # Run the stdin listener in the event loop
        loop = asyncio.get_event_loop()
        user_finished = loop.run_in_executor(None, stdin_listener)

        # Wait for user to indicate they are done listening for messages
        await user_finished

    except Exception as e:
        print ( "Unexpected error %s " % e )
    
    finally:
        # Cancel listening
        listeners.cancel()

        # Finally, disconnect
        await module_client.disconnect()
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())