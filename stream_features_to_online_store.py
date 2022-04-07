from datetime import datetime

import grpc
import pandas as pd
from feast import FeatureStore
from feast.protos.feast.serving.ServingService_pb2 import (
    FeatureList,
    FeatureReferenceV2,
    GetOnlineFeaturesRequest,
    GetOnlineFeaturesRequestV2,
)
from feast.protos.feast.serving.ServingService_pb2_grpc import ServingServiceStub
from feast.protos.feast.types.Value_pb2 import RepeatedValue, Value

import time

# Sample logic to fetch from a local gRPC java server deployed at 6566
def fetch_java():
    channel = grpc.insecure_channel("localhost:6566")
    stub = ServingServiceStub(channel)
    feature_refs = FeatureList(val=["driver_hourly_stats:conv_rate"])
    entity_rows = {
        "driver_id": RepeatedValue(
            val=[Value(int64_val=driver_id) for driver_id in range(1001, 1003)]
        )
    }

    print(
        stub.GetOnlineFeatures(
            GetOnlineFeaturesRequest(features=feature_refs, entities=entity_rows,)
        )
    )


def run_demo():
    store = FeatureStore(repo_path=".")

    

    print("\n--- Online features ---")


    print("\n--- Simulate a stream event ingestion of the hourly stats df ---")
    event_df = pd.DataFrame.from_dict(
        {
            "driver_id": [1001],
            "event_timestamp": [datetime(2021, 5, 13, 10, 59, 42),],
            "created": [datetime(2021, 5, 13, 10, 59, 42),],
            "conv_rate": [2.0],
            "acc_rate": [3.0],
            "avg_daily_trips": [4000],
            "string_feature": "test2",
        }
    )
    print(event_df)

    for i in range(10000):
         event_df = pd.DataFrame.from_dict(
                     {
            "driver_id": [1001],
            "event_timestamp": [datetime(2021, 5, 13, 10, 59, 42),],
            "created": [datetime(2021, 5, 13, 10, 59, 42),],
            "conv_rate": [i],
            "acc_rate": [i],
            "avg_daily_trips": [4000],
            "string_feature": "test2",
                      } 
                                            )

         store.write_to_online_store("driver_hourly_stats", event_df)

         print("\n--- Online features again with updated values from a stream push---")
         print(event_df)


         event_df_new = pd.DataFrame.from_dict(
                     {
            "driver_id": [1+i],
            "event_timestamp": [datetime(2021, 5, 13, 10, 59, 42),],
            "created": [datetime(2021, 5, 13, 10, 59, 42),],
            "conv_rate": [i],
            "acc_rate": [i],
            "avg_daily_trips": [4000],
            "string_feature": "test2",
                      }
                                            )

         store.write_to_online_store("driver_hourly_stats", event_df_new)

         print("\n--- Online features again with new entity  values from a stream push---")
         print(event_df_new)

                        
         time.sleep(2)
    
    

if __name__ == "__main__":
    run_demo()
