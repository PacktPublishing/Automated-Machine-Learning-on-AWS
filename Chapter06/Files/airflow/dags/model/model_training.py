import argparse
import os
import numpy as np
import pandas as pd

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from tensorflow.keras.optimizers import Adam
from sklearn import preprocessing

tf.get_logger().setLevel("ERROR")

if __name__ == "__main__":
    print(f"Tensorflow Version: {tf.__version__}")
    column_names = ["rings", "sex", "length", "diameter", "height", "whole_weight", "shucked_weight", "viscera_weight", "shell_weight"]
    parser = argparse.ArgumentParser()
    parser.add_argument("--epochs", type=int, default=2)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--model-dir", type=str, default=os.environ["SM_MODEL_DIR"])
    parser.add_argument("--training", type=str, default=os.environ["SM_CHANNEL_TRAINING"])
    args, _ = parser.parse_known_args()
    epochs = args.epochs
    batch_size = args.batch_size
    training_path = args.training
    model_path = args.model_dir
    train_data = pd.read_csv(os.path.join(training_path, "training.csv"), sep=",", names=column_names)
    val_data = pd.read_csv(os.path.join(training_path, "validation.csv"), sep=",", names=column_names)
    train_y = train_data["rings"].to_numpy()
    train_X = train_data.drop(["rings"], axis=1).to_numpy()
    val_y = val_data["rings"].to_numpy()
    val_X = val_data.drop(["rings"], axis=1).to_numpy()
    train_X = preprocessing.normalize(train_X)
    val_X = preprocessing.normalize(val_X)
    network_layers = [
        Dense(64, activation="relu", kernel_initializer="normal", input_dim=8),
        Dense(64, activation="relu"),
        Dense(1, activation="linear")
    ]
    model = Sequential(network_layers)
    model.compile(optimizer="adam", loss="mse", metrics=["mae", "accuracy"])
    model.summary()
    model.fit(
        train_X,
        train_y,
        validation_data=(val_X, val_y),
        batch_size=batch_size,
        epochs=epochs,
        shuffle=True,
        verbose=1
    )
    
    model.save(os.path.join(model_path, "model.h5"))
    model_version = 1
    export_path = os.path.join(model_path, str(model_version))
    tf.keras.models.save_model(
        model,
        export_path,
        overwrite=True,
        include_optimizer=True,
        save_format=None,
        signatures=None,
        options=None
    )