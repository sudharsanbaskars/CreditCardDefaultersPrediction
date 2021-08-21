import pandas as pd
from sklearn.model_selection import train_test_split

from Application_Logging.logger import App_Logger
from Training_DataPreprocessing.preprocessing import Preprocessor
from FileOperations.file_methods import File_Operation
from BestModelFinder.tuner import Model_Finder


class TrainModel:
    def __init__(self):
        self.logging_file_name = 'TrainModel.txt'
        self.log_writer = App_Logger()
        self.preprocessor = Preprocessor()
        self.model_finder = Model_Finder()
        self.model_saver = File_Operation()

    def model_training(self):
        try:
            path_for_data = 'Training_InputFileFromDB/InputFile.csv'
            data = pd.read_csv(path_for_data)
            self.log_writer.log(self.logging_file_name, "Got the input csv file!!")

            data.sort_values(by="ID", inplace=True, ignore_index=True)

            # drop the ID column
            data.drop("ID", axis="columns", inplace=True)

            # Seperating the data into X and Y
            X, Y = self.preprocessor.separate_target_feature(data, "default_payment_next_month")

            # check if missing values are present in the dataset
            is_null_present, cols_with_missing_values = self.preprocessor.is_null_present(X)

            # if missing values are there, replace them appropriately.
            if (is_null_present):
                X = self.preprocessor.impute_missing_values(X, cols_with_missing_values, imputer='median')  # missing value imputation

            # Handling imbalanced dataset
            sampled_X, sampled_y = self.preprocessor.handle_imbalanced_data(X,Y)


            # splitting the data into train and test data
            X_train, X_test, y_train, y_test = train_test_split(sampled_X, sampled_y, test_size=0.3, random_state=20)

            # Scaling the data
            X_train = self.preprocessor.scale_numerical_columns(X_train)
            X_test = self.preprocessor.scale_numerical_columns(X_test)
            print(X_train)
            print(X_train.columns)
            print(X_train.shape())
            # Finding the Best model
            self.log_writer.log(self.logging_file_name, "Entering the best model finder")
            best_model, model_name = self.model_finder.get_best_model(X_train, y_train, X_test, y_test)

            self.log_writer.log(self.logging_file_name, "The best model is "+str(best_model))

            # saving the best model
            saved_status = self.model_saver.save_model(best_model, str(model_name)+'.sav')

            self.log_writer.log(self.logging_file_name, str(saved_status))


        except Exception as e:
            self.log_writer.log(self.logging_file_name,
                                "model_training Failed.Exception is "+ str(e))
