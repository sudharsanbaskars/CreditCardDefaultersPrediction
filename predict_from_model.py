import numpy as np
import pandas as pd
from FileOperations.file_methods import File_Operation
# from Application_Logging.logger import App_Logger

from Prediction_DataPreprocessing.preprocessing import Preprocessor
from Prediction_RawDataValidation.raw_data_validation import RawDataValidation


class prediction:

    def __init__(self,path):
        # self.file_object = "Prediction_predictFromModel.txt"
        # self.log_writer = App_Logger()
        self.pred_data_val = RawDataValidation(path)
        self.preprocessor = Preprocessor()
        self.model_loader = File_Operation()

    def predictionFromModel(self):

        try:
            self.pred_data_val.deletePredictionFile() #deletes the existing prediction file from last run!
            # self.log_writer.log(self.file_object,'Start of Prediction')

            df_path = 'Prediction_InputFileAfterValidation/input_file.csv'
            df = pd.read_csv(df_path)
            # self.log_writer.log(self.file_object, "Got the validated CSV file!!")

            # Order the column names in alphabetical order
            data = self.preprocessor.order_columns_alphabetical_order(df)

            # check if missing values are present in the dataset
            is_null_present, cols_with_missing_values = self.preprocessor.is_null_present(data)

            # if missing values are there, replace them appropriately.
            if (is_null_present):
                data = self.preprocessor.impute_missing_values(data, cols_with_missing_values, imputer="median")  # missing value imputation

            # Removing the unwanted column "ID"
            data = self.preprocessor.remove_unwanted_columns(data, "ID")

            # Proceeding with more data pre-processing steps
            data = self.preprocessor.scale_numerical_columns(data)
#             print(data.columns)
            # print(data)


            # importing the model
            model = self.model_loader.load_model('Models/xgboost.sav')

            result = model.predict(data)
            # print(result)

            return result


        except Exception as ex:
            # self.log_writer.log(self.file_object, 'Error occured while running the prediction!! Error:: %s' % ex)
            raise ex





