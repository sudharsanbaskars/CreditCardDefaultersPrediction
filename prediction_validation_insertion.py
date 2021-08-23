# Prediction

import os
import shutil
import pandas as pd
from Application_Logging.logger import App_Logger
from Prediction_RawDataValidation.raw_data_validation import RawDataValidation


class PredictionFilesValidation:


    def __init__(self, path):
        self.file_name = 'PredictionFilesValidation.txt'
        self.log_writer = App_Logger()
        self.raw_data = RawDataValidation(path)


    def prediction_validation(self):
        try:
            self.log_writer.log(self.file_name, "Entered into PredictionFilesValidation for Prediction")
            # extracting values from prediction schema
            noofcolumns, column_names= self.raw_data.values_from_schema()

            # validating column length in the file
            self.log_writer.log(self.file_name, "Started Validating the columns length")
            is_column_length_validated = self.raw_data.validate_column_length(noofcolumns)

            if is_column_length_validated == False:
                return False

            # validating if any column has all values missing
            self.log_writer.log(self.file_name, "Validating missing values in the whole column")
            self.raw_data.validateMissingValuesInWholeColumn()

            # moving the file from good data folder to final prediction folder!
            self.log_writer.log(self.file_name, "Entering moveGoodFilesToFinalPrediction!")
            self.raw_data.moveGoodFilesToFinalPrediction()

            # Delete the good data folder after loading files in table
            self.log_writer.log(self.file_name, "Deleting Existing Good Data Folder")
            self.raw_data.deleteExistingGoodDataRawFolder()

            # Move the bad files to archive folder
            self.log_writer.log(self.file_name, "Moving bad data folder to Archive")
            self.raw_data.moveBadFilesToArchiveBad()

            self.raw_data.deleteExistingBadDataPredictionFolder()

            return True

        except Exception as e:
            self.log_writer.log(self.file_name, "Something went wrong in TrainingFiles Validation.The exception is "+str(e))
            raise e

