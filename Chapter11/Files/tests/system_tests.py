import os
import requests
import json

test_data = {
    'length': '0.455',
    'diameter': '0.365',
    'height': '0.095',
    'whole_weight': '0.514',
    'shucked_weight': '0.2245',
    'viscera_weight': '0.101',
    'shell_weight': '0.15',
    'sex': 'M'
}


def test_website():
    with requests.get(os.environ["WEBSITE_URL"]) as response:
        assert response.status_code == 200
        assert response.headers["Content-Type"] == "text/html"

def test_prediction():
    with requests.post(os.environ["API_URL"]+"api/predict", json=test_data) as response:
        assert response.status_code == 200
        assert response.headers["Content-Type"] == "application/json"
        assert "We've calcuated that the Abalone has" in json.loads(response.content)["message"]

def test_errors():
    with requests.get(os.environ["API_URL"]+"api/predict") as response:
        assert response.status_code == 404
        assert json.loads(response.content)["message"] == "Not Found"
    with requests.post(os.environ["API_URL"]+"api/predict") as response:
        assert response.status_code == 500
