import os
import json
from confluent_kafka import Consumer, Producer
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')

producer_conf = {'bootstrap.servers': KAFKA_BROKER}
producer = Producer(producer_conf)

consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'orchestrator_group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe([
    'training_completed',
    'demand_predictions',
    'inventory_updates',
    'price_updates',
    'procurement_reports',
    'prediction_error_alert'
])

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.info(f"Orchestrated event to {msg.topic()}")

def emit_cdc_event(event_type, data):
    """Emit CDC events to trigger agent workflows"""
    cdc_event = {
        "event_type": event_type,
        "timestamp": datetime.now().isoformat(),
        "data": data
    }
    
    producer.produce(
        'database_changes',
        key=event_type.encode('utf-8'),
        value=json.dumps(cdc_event).encode('utf-8'),
        callback=delivery_report
    )
    producer.poll(0)

logger.info("Orchestrator started. Coordinating agent workflows...")

state = {
    'last_training': None,
    'pending_predictions': {},
    'agent_states': {},
    'last_procurement_report': None,
}

try:
    while True:
        msg = consumer.poll(1.0)
        
        if msg is None:
            continue
        if msg.error():
            logger.error(f"Consumer error: {msg.error()}")
            continue
        
        topic = msg.topic()
        data = json.loads(msg.value().decode('utf-8'))
        
        logger.info(f"[Orchestrator] Received from {topic}: {data}")
        
        if topic == 'training_completed':
            state['last_training'] = data['timestamp']
            logger.info(f"Training completed: {data['successful_trainings']}/{data['total_products']} models")
            
            # Notify demand agents to refresh their predictions
            emit_cdc_event('ModelsUpdated', {
                'timestamp': data['timestamp'],
                'models_count': data['successful_trainings']
            })
            
        elif topic == 'demand_predictions':
            product_id = data.get('product_id')
            state['pending_predictions'][product_id] = data
            logger.info(f"Demand prediction for {product_id}: {data.get('predicted_demand')}")
            
            # Forward to inventory agent for processing
            producer.produce(
                'demand_predictions',
                key=product_id.encode('utf-8'),
                value=json.dumps(data).encode('utf-8'),
                callback=delivery_report
            )
            producer.poll(0)
            
        elif topic == 'inventory_updates':
            product_id = data.get('product_id')
            logger.info(f"Inventory update for {product_id}: {data.get('action')}")
            
            # Forward to pricing agent
            producer.produce(
                'inventory_updates',
                key=product_id.encode('utf-8'),
                value=json.dumps(data).encode('utf-8'),
                callback=delivery_report
            )
            producer.poll(0)
            
        elif topic == 'price_updates':
            product_id = data.get('product_id')
            logger.info(f"Price update for {product_id}: {data.get('new_price')}")
            
            # Log final state
            state['agent_states'][product_id] = {
                'prediction': state['pending_predictions'].get(product_id, {}),
                'price': data.get('new_price'),
                'updated_at': datetime.now().isoformat()
            }

        elif topic == 'procurement_reports':
            state['last_procurement_report'] = {
                'updated_at': datetime.now().isoformat(),
                'horizons': [r.get('horizon_months') for r in data.get('reports', [])],
                'products_limit': data.get('products_limit')
            }
            logger.info(
                "Procurement report received: horizons=%s, limit=%s",
                state['last_procurement_report']['horizons'],
                state['last_procurement_report']['products_limit'],
            )

        elif topic == 'prediction_error_alert':
            product_id = data.get('product_id')
            logger.warning(
                "Prediction error alert for %s (reason=%s, stockout_rate=%s)",
                product_id,
                data.get('reason'),
                data.get('stockout_rate'),
            )

            emit_cdc_event('PredictionErrorAlert', {
                'product_id': product_id,
                'reason': data.get('reason'),
                'requested_by': data.get('source_agent', 'procurement_agent'),
                'timestamp': data.get('timestamp', datetime.now().isoformat()),
            })
            
except KeyboardInterrupt:
    logger.info("Shutting down Orchestrator")
finally:
    consumer.close()
    producer.flush()
