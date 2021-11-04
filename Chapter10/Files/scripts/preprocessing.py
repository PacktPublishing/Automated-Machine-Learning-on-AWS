import os
import boto3
import numpy as np
import pandas as pd
import awswrangler as wr
from sklearn.utils import shuffle
from botocore.exceptions import ClientError

boto3.setup_default_session(region_name=os.environ["AWS_REGION"])
sm = boto3.client("sagemaker")


def get_featurestore_params(feature_group_name):
    try:
        response = sm.describe_feature_group(
            FeatureGroupName=feature_group_name
        )
        return response["OfflineStoreConfig"]["DataCatalogConfig"]["Database"], response["OfflineStoreConfig"]["DataCatalogConfig"]["TableName"]
    except ClientError as e:
        error_message = e.response["Error"]["Message"]
        print(error_message)
        raise Exception(error_message)


if __name__ == "__main__":
    base_dir = "/opt/ml/processing"
    print('Loading "raw" data')
    fg_name = os.environ["GROUP_NAME"]
    print(f"Using Feature Group: {fg_name}")
    columns = ["rings", "length", "diameter", "height", "whole_weight", "shucked_weight", "viscera_weight", "shell_weight", "sex_f", "sex_i", "sex_m"]
    database, table = get_featurestore_params(fg_name)
    print("Querying Feature Store Data")
    query_string = f'SELECT {",".join(columns)} FROM "{table}" WHERE is_deleted=false;'
    featurestore_df = wr.athena.read_sql_query(query_string, database=database, ctas_approach=False)
    print("Shuffling Data")
    X = shuffle(featurestore_df).to_numpy()
    print("Spliting the data into training, validation and testing datasets ...")
    training, validation, testing = np.split(X, [int(.8*len(X)), int(.95*len(X))])
    print("Saving datasets to S3")
    pd.DataFrame(training).to_csv(f"{base_dir}/output/training/training.csv", header=False, index=False)
    pd.DataFrame(validation).to_csv(f"{base_dir}/output/training/validation.csv", header=False, index=False)
    pd.DataFrame(testing).to_csv(f"{base_dir}/output/testing/testing.csv", header=False, index=False)
