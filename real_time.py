import pandas as pd
import random
from faker import Faker
import asyncio
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
import json
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)

# Initialize Faker
fake = Faker()

# Define the number of records
num_records = 100

# Define possible values for some fields
countries = ['France', 'Spain', 'Germany']
genders = ['Male', 'Female']

# Generate customer data
def generate_churn_data(num_records):
    data = []
    for _ in range(num_records):
        customer_id = random.randint(10000000, 99999999)
        credit_score = random.randint(300, 850)
        country = random.choice(countries)
        gender = random.choice(genders)
        age = random.randint(18, 90)
        tenure = random.randint(0, 10)
        balance = round(random.uniform(0, 250000), 2)
        products_number = random.randint(1, 4)
        credit_card = random.randint(0, 1)
        active_member = random.randint(0, 1)
        estimated_salary = round(random.uniform(5000, 200000), 2)
        churn = random.randint(0, 1)

        data.append({
            'customer_id': customer_id,
            'credit_score': credit_score,
            'country': country,
            'gender': gender,
            'age': age,
            'tenure': tenure,
            'balance': balance,
            'products_number': products_number,
            'credit_card': credit_card,
            'active_member': active_member,
            'estimated_salary': estimated_salary,
            'churn': churn
        })
    
    return data

async def send_data_to_eventhub(data):
    # Event Hub connection string and client initialization
    connection_str = 'YOUR CONNECTION STRING'
    eventhub_name = 'YOUR EVEBTHUB NAME'
    
    try:
        producer = EventHubProducerClient.from_connection_string(
            conn_str=connection_str,
            eventhub_name=eventhub_name
        )
        async with producer:
            try:
                event_data_batch = await producer.create_batch()
                event_data_batch.add(EventData(json.dumps(data)))
                await producer.send_batch(event_data_batch)
                logging.info('Data sent successfully to Event Hub')
            except Exception as e:
                logging.error(f"An error occurred while sending data: {e}")
    except Exception as e:
        logging.error(f"Failed to initialize EventHubProducerClient: {e}")

# Running the async function using asyncio.run
if __name__ == "__main__":
    try:
        # Generate the churn data
        data = generate_churn_data(num_records)
        
        # Send the data to Azure Event Hub
        asyncio.run(send_data_to_eventhub(data))
    except KeyboardInterrupt:
        logging.info('Process interrupted')
