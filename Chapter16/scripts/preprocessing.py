import os
import tempfile
import boto3

import numpy as np
import pandas as pd
import awswrangler as wr

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from botocore.exceptions import ClientError


boto3.setup_default_session(region_name=os.environ['AWS_REGION'])
sm = boto3.client('sagemaker')

def confirm_featurestore(model_name):
    print('Checking for existing feature store ...')
    response = sm.list_feature_groups(
        NameContains=model_name
    )
    if response['FeatureGroupSummaries'] == []:
        return None
    else:
        return response['FeatureGroupSummaries'][0]['FeatureGroupName']


def get_featurestore_params(feature_group_name):
    try:
        response = sm.describe_feature_group(
            FeatureGroupName=feature_group_name
        )
        return response['OfflineStoreConfig']['DataCatalogConfig']['Database'], response['OfflineStoreConfig']['DataCatalogConfig']['TableName']
    except ClientError as e:
        error_message = e.response['Error']['Message']
        print(error_message)
        raise Exception(error_message)


def merge_two_dicts(x, y):
    z = x.copy()
    z.update(y)
    return z


# Since we get a headerless CSV file we specify the column names here.
feature_columns_names = [
    'sex',
    'length',
    'diameter',
    'height',
    'whole_weight',
    'shucked_weight',
    'viscera_weight',
    'shell_weight',
]
label_column = 'rings'

feature_columns_dtype = {
    'sex': str,
    'length': np.float64,
    'diameter': np.float64,
    'height': np.float64,
    'whole_weight': np.float64,
    'shucked_weight': np.float64,
    'viscera_weight': np.float64,
    'shell_weight': np.float64
}
label_column_dtype = {'rings': np.float64}

new_headers = [
    'rings',
    'length',
    'diameter',
    'height',
    'whole_weight',
    'shucked_weight',
    'viscera_weight',
    'shell_weight',
    'sex_F',
    'sex_I',
    'sex_M'
]

if __name__ == '__main__':
    base_dir = '/opt/ml/processing'

    # Load the 'raw' data
    print('Loading "raw" data ...')
    df = pd.read_csv(
        f'{base_dir}/input/data/abalone.csv',
        header=None, 
        names=feature_columns_names + [label_column],
        dtype=merge_two_dicts(feature_columns_dtype, label_column_dtype)
    )

    # Scale numerical features
    numeric_features = list(feature_columns_names)
    numeric_features.remove('sex')
    numeric_transformer = Pipeline(
        steps=[
            ('imputer', SimpleImputer(strategy='median')),
            ('scaler', StandardScaler())
        ]
    )

    # Catagorical encoding of 'sex' feature
    categorical_features = ['sex']
    categorical_transformer = Pipeline(
        steps=[
            ('imputer', SimpleImputer(strategy='constant', fill_value='missing')),
            ('onehot', OneHotEncoder(handle_unknown='ignore'))
        ]
    )

    # Data pre-processing pipeline
    preprocess = ColumnTransformer(
        transformers=[
            ('num', numeric_transformer, numeric_features),
            ('cat', categorical_transformer, categorical_features)
        ]
    )
    
    # Extract the labels from the 'raw' data
    y = df.pop('rings')

    # Process features
    print('Proceseing "raw" data ...')
    X_pre = preprocess.fit_transform(df)
    y_pre = y.to_numpy().reshape(len(y), 1)

    # Rejoin the 'labels' with the processed data
    X = np.concatenate((y_pre, X_pre), axis=1)

    # Get feature store data, if it exists
    # NOTE: This data should already be pre-processed
    featurestore_name = confirm_featurestore(os.environ['MODEL_NAME'])
    if featurestore_name != None:
        print(f'Using Feature Store: {featurestore_name}')
        database, table = get_featurestore_params(featurestore_name)
        query_string = f'SELECT {",".join(new_headers)} FROM "{table}"'
        print('Querying Feature Store Data ...')
        featurestore_df = wr.athena.read_sql_query(query_string, database=database, ctas_approach=False)
        raw_df = pd.DataFrame(X, columns=new_headers)
        print('Combining "raw" data with feature store data ...')
        X = pd.concat([raw_df, featurestore_df]).to_numpy()
    
    # Shuffle the data
    print('Shuffling the data ...')
    np.random.shuffle(X)

    # (80%, 15%, 5%) train/validation/test split
    print('Spliting the data into training, validation and testing datasets ...')
    training, validation, testing = np.split(X, [int(.8*len(X)), int(.95*len(X))])

    # Create the training and testing datasets for the SageMaker training Job
    print('Saving datasets ...')
    pd.DataFrame(training).to_csv(f'{base_dir}/output/training/training.csv', header=False, index=False)
    pd.DataFrame(validation).to_csv(f'{base_dir}/output/training/validation.csv', header=False, index=False)
    pd.DataFrame(testing).to_csv(f'{base_dir}/output/testing/testing.csv', header=False, index=False)

    # NOTE: Below is the original baseline for Data Quality monitoring using the 'traning' dataset (with headers)
    # pd.DataFrame(training).to_csv(f'{base_dir}/output/baseline/baseline.csv', header=header, index=False)
    
    # NOTE: For Model Quality Monitoring, the 'baseline' file is the predictions and labels. Creating the baseline
    #       will now happen in the 'evaluation step' for the workflow instead of the 'preprocessing step'. Below
    #       is the original methodology to create a separate basleine using the testing data, which is duplicated 
    #       in the 'evaluation step'.
    # pd.DataFrame(testing).to_csv(f'{base_dir}/output/baseline/baseline.csv', header=header, index=False)