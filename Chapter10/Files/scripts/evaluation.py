import json
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
    print("Loading Model")
    
    model_path = os.path.join(base_dir, "model/model.tar.gz")
    with tarfile.open(model_path) as tar:
        tar.extractall(".")
    
    model = tf.keras.models.load_model("model.h5")
    model.compile(optimizer="adam", loss="mse")
    return model


def save_report(directory, report):
    print("Saving Evaluation Report")
    pathlib.Path(directory).mkdir(parents=True, exist_ok=True)
    evaluation_path = f"{directory}/evaluation.json"
    with open(evaluation_path, "w") as f:
        f.write(json.dumps(report))

def save_baseline(directory, predictions, labels):
    print("Saving Evaluation Quality Baseline")
    pathlib.Path(directory).mkdir(parents=True, exist_ok=True)
    baseline_path = f"{directory}/baseline.csv"
    baseline_dict = {"prediction": predictions, "label": labels}
    pd.DataFrame(baseline_dict).to_csv(baseline_path, header=True, index=False)


def evaluate_model(base_dir, model):
    print("Evaluating Model")
    truths = []
    predictions = []
    column_names = [
        "rings",
        "length", 
        "diameter",
        "height",
        "whole_weight",
        "shucked_weight",
        "viscera_weight",
        "shell_weight",
        "sex_F",
        "sex_I",
        "sex_M"
    ]
    data_path = os.path.join(base_dir, "data/testing.csv")
    data = pd.read_csv(data_path, names=column_names)
    y = data["rings"].to_numpy()
    X = data.drop(["rings"], axis=1).to_numpy()
    X = preprocessing.normalize(X)
    for row in range(len(X)):
        payload = [X[row].tolist()]
        result = model.predict(payload)
        print(f"Result: {result[0][0]}")
        predictions.append(float(result[0][0]))
        truths.append(float(y[row]))
    return truths, predictions


if __name__ == "__main__":
    input_dir = "/opt/ml/processing/input"
    output_dir = "/opt/ml/processing/output/evaluation"
    baseline_dir = "/opt/ml/processing/output/baseline"
    model = load_model(input_dir)
    y, y_pred = evaluate_model(input_dir, model)
    save_baseline(baseline_dir, y_pred, y)
    mse = mean_squared_error(y, y_pred)
    print(f"Mean Squared Error: {mse}")
    rmse = mean_squared_error(y, y_pred, squared=False)
    print(f"Root Mean Squared Error: {rmse}")
    std = np.std(np.array(y) - np.array(y_pred))
    print(f"Standard Deviation: {std}")
    report_dict = {
        "regression_metrics": {
            "rmse": {
                "value": rmse,
                "standard_deviation": std
            },
            "mse": {
                "value": mse,
                "standard_deviation": std
            },
        },
    }
    save_report(output_dir, report_dict)
