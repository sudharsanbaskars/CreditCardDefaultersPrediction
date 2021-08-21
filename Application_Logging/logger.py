from datetime import datetime

class App_Logger:
    """This class is used for logging purpose"""

    def __init__(self):
        pass

    def log(self, file_name, log_message):
        self.now = datetime.now()
        self.date = self.now.date()
        self.current_time = self.now.strftime("%H:%M:%S")
        path = "F:/Data Science & AI/Internship Projects/CreditCardDefaulterPrediction/Training_Logs"
        with open(path+"/"+str(file_name), 'a+') as f:
            f.write(str(self.date)+ "\t\t" + str(self.current_time) + "\t\t"+str(log_message)+"\n")
            f.close()
        print(str(self.date)+ "\t\t" + str(self.current_time) + "\t\t" + str(log_message)+"\n")


