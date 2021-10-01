import json
import os
import tarfile
import pandas as pd
import tensorflow as tf
from sklearn import preprocessing
def load_model(model_path):
    model = tf.keras.models.load_model(os.path.join(model_path, "model.h5"))
    model.compile(optimizer="adam", loss="mse")
    return model

def evaluate_model(prefix, model):
    column_names = ["rings", "sex", "length", "diameter", "height", "whole_weight", "shucked_weight", "viscera_weight", "shell_weight"]
    input_path = os.path.join(prefix, "processing/testing")
    output_path = os.path.join(prefix, "processing/evaluation")
    predictions = []
    truths = []
    test_df = pd.read_csv(os.path.join(input_path, "testing.csv"), names=column_names)
    y = test_df["rings"].to_numpy()
    X = test_df.drop(["rings"], axis=1).to_numpy()
    X = preprocessing.normalize(X)
    for row in range(len(X)):
        payload = [X[row].tolist()]
        result = model.predict(payload)
        print(result[0][0])
        predictions.append(float(result[0][0]))
        truths.append(float(y[row]))
    report = {
            "GroundTruth": truths,
            "Predictions": predictions
    }
    with open(os.path.join(output_path, "evaluation.json"), "w") as f:
        f.write(json.dumps(report))


if __name__ == "__main__":
    print("Extracting model archive")
    prefix = "/opt/ml"
    model_path = os.path.join(prefix, "model")
    tarfile_path = os.path.join(prefix, "processing/model/model.tar.gz")
    with tarfile.open(tarfile_path) as tar:
        tar.extractall(path=model_path)
    print("Loading Trained Model")
    model = load_model(model_path)
    print("Evaluating Trained Model")
    evaluate_model(prefix, model)
    print("Done!")