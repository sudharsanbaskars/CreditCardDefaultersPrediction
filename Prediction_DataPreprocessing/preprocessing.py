# Prediction

import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler


from Application_Logging.logger import App_Logger


class Preprocessor:

    def __init__(self):
        self.file_object = 'Prediction_Preprocessing.txt'
        self.logger_object = App_Logger()
        pass

    def order_columns_alphabetical_order(self, df):
        try:
            df.sort_index(axis="columns", inplace=True)
            self.logger_object.log(self.file_object, "Columns are ordered Succesfully")
            return df

        except Exception as e:
            self.logger_object.log(self.file_object, "Failed in ordering the columns in prediction file "+str(e))
            raise e

    def remove_unwanted_spaces(self,data):

        self.logger_object.log(self.file_object, 'Entered the remove_unwanted_spaces method of the Preprocessor class')
        self.data = data
        try:
            self.df_without_spaces = self.data.apply(
                lambda x: x.str.strip() if x.dtype == "object" else x)  # drop the labels specified in the columns
            self.logger_object.log(self.file_object,'Unwanted spaces removal Successful.Exited the remove_unwanted_spaces method of the Preprocessor class')
            return self.df_without_spaces

        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in remove_unwanted_spaces method of the Preprocessor class. Exception message:  ' + str(e))

            self.logger_object.log(self.file_object,'unwanted space removal Unsuccessful. Exited the remove_unwanted_spaces method of the Preprocessor class')

            raise Exception()

    def impute_missing_values(self, data, cols_with_missing_values, imputer='mode'):
        """
                    Method Name: impute_missing_values
                    Description: This method replaces all the missing values in the Dataframe using KNN Imputer.
                    Output: A Dataframe which has all the missing values imputed.
                    On Failure: Raise Exception

        """
        self.logger_object.log(self.file_object, 'Entered the impute_missing_values method of the Preprocessor class')
        self.data= data
        self.cols_with_missing_values=cols_with_missing_values
        try:
            for col in self.cols_with_missing_values:
                if imputer == 'mean':
                    self.data[col] = self.data.fillna(self.data[col].mean())
                elif imputer == 'mode':
                    self.data[col] = self.data.fillna(self.data[col].mode()[0])
                else:
                    self.data[col] = self.data.fillna(self.data[col].median())

            self.logger_object.log(self.file_object, 'Imputing missing values Successful. Exited the impute_missing_values method of the Preprocessor class')
            return self.data

        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in impute_missing_values method of the Preprocessor class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,'Imputing missing values failed. Exited the impute_missing_values method of the Preprocessor class')
            raise Exception()

    def remove_unwanted_columns(self,data,columns):

        self.logger_object.log(self.file_object, 'Entered the remove_columns method of the Preprocessor class')
        self.data = data
        self.columns = columns
        try:
            self.useful_data = self.data.drop(labels=self.columns, axis=1)  # drop the labels specified in the columns
            self.logger_object.log(self.file_object, 'Column removal Successful.Exited the remove_columns method of the Preprocessor class')
            return self.useful_data
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in remove_columns method of the Preprocessor class. Exception message:  '+str(e))
            self.logger_object.log(self.file_object,'Column removal Unsuccessful. Exited the remove_columns method of the Preprocessor class')

            raise Exception()


    def is_null_present(self,data):

        self.logger_object.log(self.file_object, 'Entered the is_null_present method of the Preprocessor class')
        self.data=data
        self.null_present = False
        self.cols_with_missing_values = []
        self.cols = self.data.columns
        try:
            self.null_counts = self.data.isna().sum()  # check for the count of null values per column
            for i in range(len(self.null_counts)):
                if self.null_counts[i] > 0:
                    self.null_present = True
                    self.cols_with_missing_values.append(self.cols[i])
            if (self.null_present):  # write the logs to see which columns have null values
                self.dataframe_with_null = pd.DataFrame()
                self.dataframe_with_null['columns'] = self.data.columns
                self.dataframe_with_null['missing values count'] = np.asarray(self.data.isna().sum())
                self.dataframe_with_null.to_csv('preprocessing_data/null_values.csv') # storing the null column information to file
            self.logger_object.log(self.file_object,'Finding missing values is a success.Data written to the null values file. Exited the is_null_present method of the Preprocessor class')
            return self.null_present, self.cols_with_missing_values
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in is_null_present method of the Preprocessor class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,'Finding missing values failed. Exited the is_null_present method of the Preprocessor class')
            raise Exception()

    def scale_numerical_columns(self,data):

        self.logger_object.log(self.file_object,'Entered the scale_numerical_columns method of the Preprocessor class')
        self.data=data

        try:
            self.num_df = self.data.select_dtypes(include=['int64']).copy()
            self.scaler = StandardScaler()
            self.scaled_data = self.scaler.fit_transform(self.num_df)#scaling the numerical values
            self.scaled_num_df = pd.DataFrame(data=self.scaled_data, columns=self.num_df.columns)

            self.logger_object.log(self.file_object, 'scaling for numerical values successful. Exited the scale_numerical_columns method of the Preprocessor class')
            return self.scaled_num_df

        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in scale_numerical_columns method of the Preprocessor class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object, 'scaling for numerical columns Failed. Exited the scale_numerical_columns method of the Preprocessor class')
            raise Exception()

    def encode_categorical_columns(self,data):

        self.logger_object.log(self.file_object, 'Entered the encode_categorical_columns method of the Preprocessor class')
        self.data=data

        try:
            self.cat_df = self.data.select_dtypes(include=['object']).copy()
            # Using the one hot encoding to encode the categorical columns to numerical ones
            for col in self.cat_df.columns:
                self.cat_df = pd.get_dummies(self.cat_df, columns=[col], prefix=[col], drop_first=True)

            self.logger_object.log(self.file_object, 'encoding for categorical values successful. Exited the encode_categorical_columns method of the Preprocessor class')
            return self.cat_df

        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in encode_categorical_columns method of the Preprocessor class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object, 'encoding for categorical columns Failed. Exited the encode_categorical_columns method of the Preprocessor class')
            raise Exception()



