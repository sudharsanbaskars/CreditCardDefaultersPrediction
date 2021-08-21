import csv
import os
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from Application_Logging.logger import App_Logger


class CassandraDBOperations:
    def __init__(self):
        self.training_good_file_path = 'F:/Data Science & AI/Internship Projects/CreditCardDefaulterPrediction/Training_ValidatedRawData/GoodDataFolder'
        # self.bad_data_folder = 'Training_ValidatedRawData/BadDataFolder'
        self.logger = App_Logger()
        self.logging_file_name = 'Cassandra_operations.txt'
        self.prediction_logging_file_name = 'Prediction_Cassandra_operations.txt'


        cloud_config = {
            'secure_connect_bundle': 'F:/Data Science & AI/Internship Projects/CreditCardDefaulterPrediction/CassandraConfigFiles/secure-connect-creditcarddefaulters.zip'
        }
        auth_provider = PlainTextAuthProvider('DONXfAhtYYqeNKIrLBrxLDCG',
                                              '7UavhtOqJkSE9b+FQFZ6ZyYpuq1MmLsZZhF,15WzSCkS8kS5L24xnRyK4m,7bgPR6Wo_G6P834dFt9yRxiC5fKjZKFiYfQ9g2uNiD3o4m_6Zfx_5KJ6WAn99lzbMZLJK')
        self.cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        self.session = self.cluster.connect()

        row = self.session.execute("select release_version from system.local").one()
        if row:
            print(row[0])
        else:
            print("An error occurred.")

        self.session.execute("USE credit_card_defaulters")


    def createTable(self, table_name):
        try:

            self.session.execute("Create Table " +str(table_name)+"(ID INT PRIMARY KEY, LIMIT_BAL DECIMAL, SEX INT, EDUCATION INT, MARRIAGE INT, AGE INT,PAY_0 INT,PAY_2 INT,PAY_3 INT,"
                                 "PAY_4 INT,PAY_5 INT,PAY_6 INT,BILL_AMT1 DECIMAL,BILL_AMT2 DECIMAL,BILL_AMT3 DECIMAL,BILL_AMT4 DECIMAL,BILL_AMT5 DECIMAL,BILL_AMT6 DECIMAL,PAY_AMT1 DECIMAL,PAY_AMT2 DECIMAL,PAY_AMT3 DECIMAL,PAY_AMT4 DECIMAL,PAY_AMT5 DECIMAL,PAY_AMT6 DECIMAL,default_payment_next_month INT)")
            self.logger.log(self.logging_file_name, "Table Created Successfully!!")

        except Exception as e:
            self.logger.log(self.logging_file_name, "Failed in Creating Table "+str(e))


    def insertIntoTableFromGoodData(self, table_name):
        try:
            # self.session.execute("USE credit_card_defaulters")
            if os.path.isdir(self.training_good_file_path):
                files = os.listdir(self.training_good_file_path)
                for file in files:
                    self.logger.log(self.logging_file_name, "Found a file in good data folder "+str(file))
                    with open(self.training_good_file_path+"/"+file, 'r') as data:
                        next(data)
                        data_csv = csv.reader(data, delimiter=',')
                        # csv reader object
                        print(data_csv)
                        all_value = []
                        self.logger.log(self.logging_file_name, "Starting insertion into the table")
                        for i in data_csv:
                            self.session.execute("INSERT INTO "+str(table_name)+"(ID,LIMIT_BAL,SEX,EDUCATION,MARRIAGE,AGE,PAY_0,PAY_2,PAY_3,PAY_4,PAY_5,PAY_6,BILL_AMT1,\
                            BILL_AMT2,BILL_AMT3,BILL_AMT4,BILL_AMT5,BILL_AMT6,PAY_AMT1,PAY_AMT2,PAY_AMT3,PAY_AMT4,PAY_AMT5,PAY_AMT6,\
                           default_payment_next_month) VALUES({ID},{LIMIT_BAL},{SEX},{EDUCATION},{MARRIAGE},{AGE},{PAY_0},{PAY_2},{PAY_3},{PAY_4},{PAY_5},\
                           {PAY_6},{BILL_AMT1},{BILL_AMT2},{BILL_AMT3},{BILL_AMT4},{BILL_AMT5},{BILL_AMT6},{PAY_AMT1},{PAY_AMT2},{PAY_AMT3},{PAY_AMT4},{PAY_AMT5},{PAY_AMT6},\
                           {default_payment_next_month})".format(ID=i[0], LIMIT_BAL=i[1], SEX=int(i[2]), EDUCATION=int(i[3]),
                                                                 MARRIAGE=int(i[4]), AGE=int(i[5]), PAY_0=int(i[6]), \
                                                                 PAY_2=int(i[7]), PAY_3=int(i[8]), PAY_4=i[9], PAY_5=int(i[10]),
                                                                 PAY_6=int(i[11]), BILL_AMT1=i[12], BILL_AMT2=i[13],
                                                                 BILL_AMT3=i[14], BILL_AMT4=i[15], BILL_AMT5=i[16],
                                                                 BILL_AMT6=i[17], PAY_AMT1=i[18], PAY_AMT2=i[19],
                                                                 PAY_AMT3=i[20], \
                                                                 PAY_AMT4=i[21], PAY_AMT5=i[22], PAY_AMT6=i[23],
                                                                 default_payment_next_month=int(i[24])))
                    print('Finished')
            else:
                self.logger.log(self.logging_file_name, "GoodDataFolder Doesn't Exist!!")

        except Exception as e:
            self.logger.log(self.logging_file_name, "Failed in inserting data into table" + str(e))


    def selectingDatafromtableintocsv(self, table_name):
        try:
            header = ['ID','AGE','BILL_AMT1','BILL_AMT2','BILL_AMT3','BILL_AMT4','BILL_AMT5','BILL_AMT6',
                      'default_payment_next_month','EDUCATION','LIMIT_BAL','MARRIAGE','PAY_0','PAY_2',
                      'PAY_3','PAY_4','PAY_5','PAY_6','PAY_AMT1','PAY_AMT2','PAY_AMT3','PAY_AMT4','PAY_AMT5',
                      'PAY_AMT6','SEX']

            i = 0
            path_to_final_csv = 'F:/Data Science & AI/Internship Projects/CreditCardDefaulterPrediction/Training_InputFileFromDB/InputFile.csv'
            self.logger.log(self.logging_file_name, "Starting Inserting the data into csv file")
            with open(path_to_final_csv, 'w') as f:
                csv_writer = csv.writer(f, delimiter=',')
                results = self.session.execute("select * from "+str(table_name)+";")
                if i == 0:
                    csv_writer.writerow(header)
                i = i + 1
                for row in results:
                    csv_writer.writerow(list(row))
            self.logger.log(self.logging_file_name, "Sucessfully Inserted data into InputFile.csv from DB!")
        except Exception as e:
            self.logger.log(self.logging_file_name, "Failed in inserting data into csv" + str(e))

##------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


    def createTableForPredictionData(self, table_name):
        try:

            self.session.execute("Create Table " +str(table_name)+"(ID INT PRIMARY KEY, LIMIT_BAL DECIMAL, SEX INT, EDUCATION INT, MARRIAGE INT, AGE INT,PAY_0 INT,PAY_2 INT,PAY_3 INT,"
                                 "PAY_4 INT,PAY_5 INT,PAY_6 INT,BILL_AMT1 DECIMAL,BILL_AMT2 DECIMAL,BILL_AMT3 DECIMAL,BILL_AMT4 DECIMAL,BILL_AMT5 DECIMAL,BILL_AMT6 DECIMAL,PAY_AMT1 DECIMAL,PAY_AMT2 DECIMAL,PAY_AMT3 DECIMAL,PAY_AMT4 DECIMAL,PAY_AMT5 DECIMAL,PAY_AMT6 DECIMAL)")
            self.logger.log(self.prediction_logging_file_name, "Table Created Successfully!!")

        except Exception as e:
            self.logger.log(self.prediction_logging_file_name, "Failed in Creating Table "+str(e))



