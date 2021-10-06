import json
import sys
import os
import pathlib
import tarfile
import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from tensorflow.keras.optimizers import Adam
from sklearn import preprocessing
from sklearn.metrics import mean_squared_error


def load_model(base_dir):
    print('Loading Model ...')
    
    # Extract 'h5' files
    model_path = os.path.join(base_dir, 'model/model.tar.gz')
    with tarfile.open(model_path) as tar:
        tar.extractall('.')
    
    # Load 'h5' keras model
    model = tf.keras.models.load_model('model.h5')
    model.compile(optimizer='adam', loss='mse')
    return model


def save_report(directory, report):
    print('Saving Evaluation Report ...')
    # Create the report directory if it doesn't exist
    pathlib.Path(directory).mkdir(parents=True, exist_ok=True)

    # Write the 'report' to file
    evaluation_path = f'{directory}/evaluation.json'
    with open(evaluation_path, 'w') as f:
        f.write(json.dumps(report))

def save_baseline(directory, predictions, labels):
    print('Saving Evaluation Quality Baseline ...')

    # Create the baseline directory if it doesn't exist
    pathlib.Path(directory).mkdir(parents=True, exist_ok=True)

    # Create a DataFrame of the predicitons and ground truth labels
    baseline_path = f'{directory}/baseline.csv'
    baseline_dict = {'prediction': predictions, 'label': labels}

    # Write the DataFrame to a 'csv' file
    pd.DataFrame(baseline_dict).to_csv(baseline_path, header=True, index=False)


def evaluate_model(base_dir, model):
    print('Evaluating Model ...')
    # Set the data parameters
    truths = []
    predictions = []
    column_names = [
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
    data_path = os.path.join(base_dir, 'data/testing.csv')

    # Read the testing data as a DataFrame
    data = pd.read_csv(data_path, names=column_names)

    # Create the ground truth labels
    y = data['rings'].to_numpy()

    # Create the prediction features and observations
    X = data.drop(['rings'], axis=1).to_numpy()
    X = preprocessing.normalize(X)
    
    # Get predictions from the model
    for row in range(len(X)):
        payload = [X[row].tolist()]
        result = model.predict(payload)
        print(f'Result: {result[0][0]}')
        predictions.append(float(result[0][0]))
        truths.append(float(y[row]))
    
    # Return the results
    return truths, predictions


if __name__ == '__main__':
    # print(f'Tensorflow Version: {tf.__version__}')
    input_dir = '/opt/ml/processing/input'
    output_dir = '/opt/ml/processing/output/evaluation'
    baseline_dir = '/opt/ml/processing/output/baseline'

    # Load the model 
    model = load_model(input_dir)

    # Evaluate the predictions
    y, y_pred = evaluate_model(input_dir, model)

    # Save the evaluation as a baseline results for Model Quality Monitoring
    save_baseline(baseline_dir, y_pred, y)
    
    # Calculate the mse
    mse = mean_squared_error(y, y_pred)
    print(f'Mean Squared Error: {mse}')
    
    # Calculate the rmse
    rmse = mean_squared_error(y, y_pred, squared=False)
    print(f'Root Mean Squared Error: {rmse}')

    # Calculate the standard deviation
    std = np.std(np.array(y) - np.array(y_pred))
    print(f'Standard Deviation: {std}')

    # Create a report for the model registry
    report_dict = {
        'regression_metrics': {
            'rmse': {
                'value': rmse,
                'standard_deviation': std
            },
            'mse': {
                'value': mse,
                'standard_deviation': std
            },
        },
    }
    # pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)
    # evaluation_path = f'{output_dir}/evaluation.json'
    # with open(evaluation_path, "w") as f:
    #     f.write(json.dumps(report_dict))

    # Save the evaluation report
    save_report(output_dir, report_dict)
