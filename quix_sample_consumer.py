from quixstreams import Application
from embedchain import App as ecApp
import os

def main():
    """
    Read data from the CSV file and publish it to Kafka
    """
    # Create an Application
    kafka_app = Application(quix_sdk_token=os.getenv("QUIX_SDK_TOKEN"),
                            consumer_group="csv_sample",
                            auto_create_topics=True)

    # Define the topic using the "output" environment variable
    topic_name = os.environ["input"]
    topic = kafka_app.topic(topic_name)

    os.getenv("OPENAI_API_KEY")
    chat_bot = ecApp()

    with kafka_app.get_consumer() as consumer:
        consumer.subscribe([topic.name])
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is not None:
                # print(f"--->\nvalue is {msg.value()}\n with key {msg.key()}\n topic is {msg.topic()}\npartition is {msg.partition()}\nlatency is {msg.latency()}\n<---")
                # Store the message in the vector database
                #url = msg.value().decode("UTF-8").split("https://")[1].split(":")[0].rsplit()
                #print(url)

                value = msg.value().decode("UTF-8")

                # Encontrar la posición de la primera comilla simple
                start = value.find("'") + 1
                # Encontrar la posición de la última comilla simple
                end = value.rfind("'")

                # Extraer la parte de la cadena que contiene el objeto JSON
                json_str = value[start:end].split("https://")[1].split(":")[0].rstrip('"')

                # Analizar la cadena JSON
                print(f"final url is {json_str}")
                chat_bot.add(json_str)
                print("Added to embedchain.")
                # Optionally commit the offset
                #consumer.store_offsets(msg.offset())

if __name__ == "__main__":
    try:
        main()
        print("Done!")
    except KeyboardInterrupt:
        print("Exiting.")