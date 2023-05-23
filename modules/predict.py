import pandas as pd
import dill
from datetime import datetime
import os
import json
from pydantic import BaseModel


pathlocal = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
path = os.environ.get('PROJECT_PATH', pathlocal)


class Form(BaseModel):
    id: int
    url: str
    region: str
    region_url: str
    price: int
    year: float
    manufacturer: str
    model: str
    fuel: str
    odometer: float
    title_status: str
    transmission: str
    image_url: str
    description: str
    state: str
    lat: float
    long: float
    posting_date: str


class Prediction(BaseModel):
    car_id: int
    pred: str


def predict() -> None:
    model_dir = f'{path}/data/models/'
    model_filename = f'{model_dir}/cars_pipe_{datetime.now().strftime("%Y%m%d%H%M")}.pkl'

    # Check if exists
    if not os.path.exists(model_filename):
        # Last model file
        model_files = sorted(os.listdir(model_dir), reverse=True)
        model_filename = None
        for file in model_files:
            if file.endswith('.pkl'):
                model_filename = file
                break
    # Some model file existence
    if model_filename is None:
        print("Pkl Error: Model file not found")
        return

    model_path = os.path.join(model_dir, model_filename)

    # Load model
    with open(model_path, 'rb') as file:
        best_pipe = dill.load(file)
    #Read data dir
    test_files = os.listdir(f'{path}/data/test')
    prediction_data = []
    for file in test_files:
        if file.endswith('.json'):
            with open(os.path.join(f'{path}/data/test', file), 'r') as json_file:
                data = json.load(json_file)
                form_data = Form(**data)
                prediction = best_pipe.predict(pd.DataFrame([form_data.dict()]))
                prediction_data.append(Prediction(car_id=form_data.id, pred=prediction[0]))

    # Create DataFrame from prediction data
    prediction_df = pd.DataFrame([entry.dict() for entry in prediction_data])

    # Save selected fields to CSV
    selected_fields = ['car_id', 'pred']
    selected_df = prediction_df[selected_fields]

    # Save predictions
    predictions_filename = f'{path}/data/predictions/predictions_{datetime.now().strftime("%Y%m%d%H%M")}.csv'
    selected_df.to_csv(predictions_filename, index=False)


if __name__ == '__main__':
    predict()


