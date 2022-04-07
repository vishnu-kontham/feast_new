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



    print("\n--- Online features again with updated values from a stream push---")
    features = store.get_online_features(
        features=[
            "driver_hourly_stats:acc_rate",
            "driver_hourly_stats:avg_daily_trips",
            "transformed_conv_rate:conv_rate_plus_val1",
            "transformed_conv_rate:conv_rate_plus_val2",
            "driver_age:driver_age",
        ],
       entity_rows=[
            {
                "driver_id":  1001,
                "val_to_add": 1000,
                 "val_to_add_2": 2000,
                  "driver_age": 25,
               },
             {
                "driver_id":  10,
                "val_to_add": 1000,
                 "val_to_add_2": 2000,
                  "driver_age": 25,
               }

            ],


    ).to_dict()
    for key, value in sorted(features.items()):
        print(key, " : ", value)


if __name__ == "__main__":
    run_demo()
