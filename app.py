import os
import csv
import shutil
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, send_from_directory, send_file
from flask_cors import CORS, cross_origin

from prediction_validation_insertion import PredictionFilesValidation
from predict_from_model import prediction

app = Flask(__name__)

app.config["csv_file"] = "Prediction_FinalResultFile/"
app.config["sample_file"] = "Prediction_SampleFile/"


@app.route('/')
@cross_origin()
def home():
    return render_template('index.html')

@app.route('/return_sample_file/')
@cross_origin()
def return_sample_file():
    sample_file = os.listdir("Prediction_SampleFile/")[0]
    return send_from_directory(app.config["sample_file"], sample_file)


@app.route('/return_file/')
@cross_origin()
def return_file():
    final_file = os.listdir("Prediction_FinalResultFile/")[0]
    return send_from_directory(app.config["csv_file"], final_file)


@app.route('/result')
@cross_origin()
def result():
    return render_template('result.html')


@app.route('/predict', methods=['POST'])
@cross_origin()
def predict():
    if request.method == 'POST':
        try:
            if 'csvfile' not in request.files:
                return render_template("invalid.html")

            message = ''
            file = request.files['csvfile']
            df = pd.read_csv(file, index_col=[0])

            path = 'Prediction_InputFileFromUser/'

            if os.path.isfile('Prediction_InputFileFromUser/input_file.csv'):
                os.remove('Prediction_InputFileFromUser/input_file.csv')

            df.to_csv('Prediction_InputFileFromUser/input_file.csv')

            pred_obj = PredictionFilesValidation(path)  # object initialization
            is_validated = pred_obj.prediction_validation()  # calling the training_validation function

            if is_validated:
                pred = prediction(path)  # object initialization
                result = pred.predictionFromModel()
                result_df = pd.DataFrame(result, columns=["default_payment_next_month"])
                # print(result_df)

                final_df = pd.read_csv(path+"input_file.csv")
                final_df["default_payment_next_month"] = result_df


                if os.path.isfile('Prediction_FinalResultFile/Result.csv'):
                    os.remove('Prediction_FinalResultFile/Result.csv')
                final_df.to_csv('Prediction_FinalResultFile/Result.csv', index=False)
                message = "Success"
            else:
                return render_template('invalid.html')
            print(message)
        except Exception as e:
            return render_template("invalid.html")



    return redirect(url_for('result'))


if __name__ == '__main__':
    app.run(debug=True)



