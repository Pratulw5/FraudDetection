from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
import json
from pyflink.datastream import RuntimeContext
from pyflink.common import Time, Types
from pyflink.datastream import OutputTag
import pickle
import redis
import psycopg2
from datetime import datetime, timedelta
from river.anomaly import HalfSpaceTrees
from river import preprocessing, anomaly
from pyflink.common import Duration


# Redis config for model + last transaction
REDIS_CONFIG = {
    "host": "redis-service",
    "port": 6379
}
#values:
# Rule 4:
HIGH_VALUE_THRESHOLD = 100000
FRIENDLINESS_THRESHOLD = 0.05
# Rule 7
SAME_AMOUNT_TIME_THRESHOLD = 1
SUSPICIOUS_REPITION_THRESHOLD = 3
# Rule 8: Mule Detection - Same receiver, many senders
MULE_SENDER_THRESHOLD = 10
MULE_TIME_WINDOW_HOURS = 1

KAFKA_PRODUCER_TOPIC = "MLdata"

# Define Kafka producer and consumer properties
kafka_props = {
    'bootstrap.servers': 'kafka:9092'
}

KAFKA_PRODUCER_TOPIC = "MLdata"
kafka_producer_props = {
    'bootstrap.servers': 'kafka:9092',
}



class RuleCheck(KeyedProcessFunction):

    def process_element(self, value, ctx):
        txn = json.loads(value)

        # Cast all fields to float (or int where appropriate)
        old_org   = float(txn["oldbalanceOrg"])
        new_org   = float(txn["newbalanceOrig"])
        old_dest  = float(txn["oldbalanceDest"])
        new_dest  = float(txn["newbalanceDest"])
        amt       = float(txn["amount"])

        # Now your rule logic with real numbers
        origin_ok = (
            (old_org == 0 and new_org == 0)
            or (old_org < amt and new_org == 0)
            or (new_org == old_org - amt)
        )
        dest_ok = (new_dest == old_dest + amt)

        txn["Rules_Check"] = int(not (origin_ok and dest_ok))
        yield json.dumps(txn)




class UnifiedScoring(KeyedProcessFunction):
    def __init__(self):
        self.output_tag = OutputTag("rule-scores-output", Types.STRING())
    def open(self, runtime_context: RuntimeContext):
        self.redis = redis.Redis(**REDIS_CONFIG)

    def loadModelHST (self, modelName):
        try:
            data = self.redis.get(modelName)
            if data:
                return pickle.loads(data)
        except Exception as e:
            print("Redis model load failed:", e)
        return preprocessing.StandardScaler() | HalfSpaceTrees(seed=42)

    def update_average_amount(self, lastCount, lastAvg, amount):
        if lastCount == 0:
            score_rule3 = 0.30
        else:
            deviation = abs(amount - lastAvg)
            score_rule3 = round(min((deviation / lastAvg), 1.0), 4)
        newCount = lastCount + 1
        newAvg = (lastAvg * lastCount + amount) / newCount
        return newAvg, newCount, score_rule3

    def get_features_r1 (self, userstats, latitude, timestamp):
        if b'last_latitude' in userstats and b'last_timestamp' in userstats:
            last_latitude = float(userstats[b'last_latitude'].decode("utf-8"))
            last_timestamp = userstats[b'last_timestamp'].decode("utf-8")
            try:
                fmt = "%d/%m/%Y %H:%M"
                t1 = datetime.strptime(timestamp, fmt)
                t0 = datetime.strptime(last_timestamp, fmt)
                time_diff = (t1 - t0).total_seconds() / 60
            except Exception as e:
                print("Timestamp parsing error:", e)
                time_diff = 0
            return {
                "latitude_diff": abs(float(latitude) - last_latitude),
                "time_diff": time_diff
            }
        else:
            return {"latitude_diff": 0, "time_diff": 0}

    def get_features_r2(self, timestamp):     
        try:
            return {"hour": datetime.strptime(timestamp, "%d/%m/%Y %H:%M").hour}
            
        except Exception as e:
            print("Hour parsing error:", e)
            return {"hour": 0}

    def get_features_r3(self, userstats):
        if b'last_Avg' in userstats and b'last_Count' in userstats:
            return  (float(userstats[b'last_Avg'].decode("utf-8")),
            int(float(userstats[b'last_Count'].decode("utf-8"))))
        return (0.0, 0)

    def get_features_r4(self, userstats, merchant_id):
        if b'merchant_txn_count_map' in userstats:
            # Decode and load the merchant txn dict
            merchant_txn_count_map = json.loads(userstats[b'merchant_txn_count_map'].decode("utf-8"))
            # Get the count for the merchant
            merchant_txn_count = merchant_txn_count_map.get(merchant_id, 0)  
            return ( merchant_txn_count_map, merchant_txn_count)
        else:
            return ({}, 0)


    def get_features_r5(self, old_balance, new_balance):
        balance_diff = old_balance - new_balance
        return {"old_balance": old_balance, "new_balance": new_balance}


    def get_features_r6(self, userstats, timestamp, amount,last_count):
        try:
            # Check for required keys
            if b'amount' in userstats and b'first_transaction_timestamp' in userstats and b'last_Count' in userstats:
                first_transaction_timestamp = userstats[b'first_transaction_timestamp'].decode()
                prev_amount = float(userstats[b'amount'].decode("utf-8"))
                

                # Calculate time difference in hours
                diff = (datetime.strptime(first_transaction_timestamp, "%d/%m/%Y %H:%M") -
                        datetime.strptime(timestamp, "%d/%m/%Y %H:%M"))
                diff_hours = diff.total_seconds() / 3600

                return {"amount" :prev_amount + amount, "Count": last_count, "Time Difference": diff_hours}
            else:
                
                return {"amount" : amount, "Count":last_count, "Time Difference": 0}
        except Exception as e:
            print("Hour parsing error:", e)
            
            prev_amount = float(userstats[b'amount'].decode("utf-8"))
            return {"amount" :prev_amount, "Count": last_count, "Time Difference": 0}

    def get_features_r7(self, amount, user_id):
        # Rule 7: Multiple similar amounts by user in 1 hour
        amount_key = f"user:{user_id}:amount:{round(amount, 2)}"
        txn_count_key = f"user:{user_id}:txn_count"
        try:
            if self.redis.exists(txn_count_key):
                self.redis.incr(txn_count_key)
            else:
                self.redis.setex(txn_count_key, timedelta(hours=SAME_AMOUNT_TIME_THRESHOLD), 1)
            current_count = int(self.redis.get(amount_key) or 0)
            total_count = int(self.redis.get(txn_count_key))
            return (current_count, total_count)
        except Exception as e:
            print(f"Redis error in Rule 7: {e}")
            return (0, 1)

    def get_features_r8(self, receiver_id, sender_id, timestamp):
        try:
            timestamp_key = f"receiver:{receiver_id}:senders"
            now = datetime.strptime(timestamp, "%d/%m/%Y %H:%M")
            
            # Track sender in Redis Set with expiry
            self.redis.sadd(timestamp_key, sender_id)
            self.redis.expire(timestamp_key, int(MULE_TIME_WINDOW_HOURS * 3600))

            # Count unique senders
            unique_senders = self.redis.scard(timestamp_key)
            return unique_senders
        except Exception as e:
            print(f"Error in Rule 8 (Mule Detection): {e}")
            return 0



    def process_element(self, value, ctx):
        try:
    # Defining Variables
            txn = json.loads(value)
            user_id = txn["nameOrig"]
            merchant_id = txn["nameDest"]
            timestamp = txn["timestamp"]
            amount = float(txn["amount"])
            old_balance = float(txn["oldbalanceOrg"])
            new_balance = float(txn["newbalanceOrig"])
            latitude = float(txn.get("latitude", 0))
            userstats = self.redis.hgetall(f'user:{user_id}')
            user_key = f"user:{user_id}"
            Rules_Check = txn["Rules_Check"]
            

# -------------------

        # Rule 1 : Anomality in location (user id, timestamp and latitude) - Global (Dual)

            # Load Model:
            locationAnomaly = self.loadModelHST("global_models:location_anomaly")

            # Transform Data:
            features_r1 = self.get_features_r1(userstats, latitude, timestamp)
            
            # Add Data To Model:
            score_rule1 = locationAnomaly.score_one(features_r1)
            locationAnomaly.learn_one(features_r1)

            # Model Save:
            self.redis.set("global_models:location_anomaly", pickle.dumps(locationAnomaly))

            # Database Update:
            self.redis.hset(user_key, mapping={
                "last_latitude": latitude,
                "last_timestamp": timestamp,
            })

    # -------------------

        # Rule 2: Anomality in time (minuites) (nameOrig, timestamp) - Personal (Present)
            
            # Load Model:
            timeAnomaly = self.loadModelHST(f"user:time_anomaly_model:{user_id}")

            # Transform Data: 
            features_r2 = self.get_features_r2(timestamp)

            # Add Data To Model:
            score_rule2 = timeAnomaly.score_one(features_r2)
            timeAnomaly.learn_one(features_r2)

            # Model Save:
            self.redis.set(f"user:time_anomaly_model:{user_id}", pickle.dumps(timeAnomaly))
            self.redis.expire(f"user:time_anomaly_model:{user_id}", 3600) # 1 hour expiry 
            # imp: the expiry restarts after each update in transaction  

            # Database Update: None (Direct Feed)

    # -------------------

        # Rule 3: Amount of transaction per user (nameOrig and amount)

            # Load Model: Artifical model

            # Transform Data:
            lastAvg, lastCount = self.get_features_r3(userstats)
            
            # Add Data To Model (Artificial):
            newAvg, newCount, score_rule3 = self.update_average_amount(lastAvg, lastCount, amount)
            
            # Model Save: NA
            
            # Database Update: 
            self.redis.hset(user_key, mapping={
                "last_Avg": newAvg,
                "last_Count": int(newCount),
            })
    # -------------------

        # Rule 4: First-time high-value transaction with a merchant (nameOrig, nameDest and amount)
            
            # Load Model: Artifical model

            # Transform Data:
            merchant_txn_count_map,merchant_txn_count = self.get_features_r4(userstats, merchant_id)
            total_txn_count = lastCount
            # total_txn is same as last count hence already increased by 1 oin previuos rule
            
            
            # Add Data To Model (Artificial):
            if amount <= HIGH_VALUE_THRESHOLD: # if the amount is not too high dont need crosscheck the merchant
                score_rule4 = 0.0
            else:# cross check if the merchant reliable using friendliness
                friendliness = (merchant_txn_count + 1) / total_txn_count if total_txn_count > 0 else 0
                score_rule4 = friendliness
            # Model Save: NA

            # Database Update: 
            # increments the merchant for the merchant id by 1
            merchant_txn_count_map[merchant_id] = merchant_txn_count_map.get(merchant_id, 0) + 1
            
            # total_txn is same as last count hence already increased by 1 oin previuos rule
            self.redis.hset(user_key, "merchant_txn_count_map", json.dumps(merchant_txn_count_map))

    # -------------------

        # Rule 5: Balance Cleanout Rule (oldBalanceOrig - newBalanceOrig) - Global (Present)
            
            # Load Model: 
            balanceCleanout = self.loadModelHST(f"global_models:balance_cleanout_model")
            
            # Transform Data:
            features_r5 = self.get_features_r5(old_balance, new_balance)
            
            # prevent division by zero

            # Add Data To Model:
            score_rule5 = balanceCleanout.score_one(features_r5)
            balanceCleanout.learn_one(features_r5)

            # Model Save:
            self.redis.set("global_models:balance_cleanout_model", pickle.dumps(balanceCleanout))

            # Database Update: None (Direct Feed)

    # -------------------

        # Rule 6: Transaction frequency Dual

            # Load model
            transactionFrequency = self.loadModelHST(f"user:transaction_frequency:{user_id}")
            
            # Trasnform Data:
            features_r6 = self.get_features_r6(userstats, timestamp, amount,lastCount)

            # Add Data To Model:
            score_rule6 = transactionFrequency.score_one(features_r6)
            transactionFrequency.learn_one(features_r6)

            # Model Save:
            self.redis.set(f"user:transaction_frequency:{user_id}", pickle.dumps(transactionFrequency))
            
            # Database Update: None (Direct Feed)
            if b'first_transaction_timestamp' not in userstats:
                self.redis.hset(f"user:{user_id}", mapping={
                    "first_transaction_timestamp": timestamp,
                    "amount": features_r6["amount"]
                })
            else:
                self.redis.hset(f"user:{user_id}", mapping={"amount": features_r6["amount"]})
        
        
        
        # Rule 7: Same Amount Multiple Times

            # Load model - Artificial
            # Trasnform Data:
            current_count, total_count = self.get_features_r7(amount, user_id)
            current_count += 1
            
            # Add Data To Model:
            score_rule7  = current_count/total_count if current_count >= SUSPICIOUS_REPITION_THRESHOLD else 0
            # Model Save:
            amount_key = f"user:{user_id}:amount:{round(amount, 2)}"
            txn_count_key = f"user:{user_id}:txn_count"
            # Database Update: None (Direct Feed)
            self.redis.setex(amount_key, timedelta(hours=SAME_AMOUNT_TIME_THRESHOLD), current_count)
            
        

        # Rule 8: Mule Detection - Same Receiver Many Senders
            unique_senders_count = self.get_features_r8(merchant_id, user_id, timestamp)
            score_rule8 = 1.0 if unique_senders_count >= MULE_SENDER_THRESHOLD else 0.0

            
    # -------------------

# Prepare score output for Kafka
            score_output = json.dumps({
                        "timestamp": timestamp,
                        "user_id": user_id,
                        "score_rule1": score_rule1,
                        "score_rule2": score_rule2,
                        "score_rule3": score_rule3,
                        "score_rule4": score_rule4,
                        "score_rule5": score_rule5,
                        "score_rule6": score_rule6,
                        "score_rule7": score_rule7,
                        "score_rule8": score_rule8,
                        "Rules_Check": Rules_Check
                    })

            ctx.output(self.output_tag, score_output)


# -------------------

        except Exception as e:
            print("Processing error:", e)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Kafka Source
    consumer = FlinkKafkaConsumer(
        topics='raw-transactions',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )
    source_stream = env.add_source(consumer)

    # Rule-based checking
    processed = (
        source_stream
        .key_by(lambda x: json.loads(x)["nameOrig"])
        .process(RuleCheck(), output_type=Types.STRING())
    )
    producer = FlinkKafkaProducer(
    topic=KAFKA_PRODUCER_TOPIC,
    serialization_schema=SimpleStringSchema(),
    producer_config=kafka_producer_props
    )
    scored.add_sink(producer).name("Kafka Rule Score Sink") 
    # Scoring
    keyed = processed.key_by(lambda x: json.loads(x)["nameOrig"])
    scoring_function = UnifiedScoring()
    scored = keyed.process(scoring_function).name("Unified Scoring")

    # Side Output Sink (if any)
    scored = keyed.process(scoring_function, output_type=Types.STRING())
    scored.add_sink(producer).name("Kafka Rule Score Sink")

    env.execute("Flink Incremental Anomaly Detection")

if __name__ == "__main__":
    main()



    