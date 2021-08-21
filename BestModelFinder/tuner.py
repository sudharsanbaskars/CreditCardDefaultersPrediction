from sklearn.linear_model import LogisticRegression
from sklearn.naive_bayes import GaussianNB
from xgboost import XGBClassifier
from sklearn.svm import SVC
from sklearn.model_selection import GridSearchCV
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score,accuracy_score

from Application_Logging.logger import App_Logger


class Model_Finder:
    """
        This Class is used for finding the best Algorithm for the given dataset
    """

    def __init__(self):
        self.file_object = 'ModelFinding.txt'
        self.logger_object = App_Logger()
        self.lor = LogisticRegression()
        self.knn = KNeighborsClassifier()
        self.rf = RandomForestClassifier()
        self.gnb = GaussianNB()
        self.svm = SVC()
        self.xgb = XGBClassifier(objective='binary:logistic', n_jobs=-1)

    def get_best_params_for_logistic_regression(self,train_x,train_y):

        self.logger_object.log(self.file_object, 'Entered the get_best_params_for_logistic_regression method of the Model_Finder class')
        self.train_x=train_x
        self.train_y=train_y

        try:

            # creating a LogisticRegression model
            self.lor = LogisticRegression()
            # training the LogisticRegression model
            self.lor.fit(self.train_x, self.train_y)
            self.logger_object.log(self.file_object, '. Exited the get_best_params_for_logistic_regression method of the Model_Finder class')

            return self.lor
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in get_best_params_for_logistic_regression method of the Model_Finder class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,'Logistic Regression failed. Exited the get_best_params_for_logistic_regression method of the Model_Finder class')
            raise Exception()

    def get_best_params_for_knn(self,train_x,train_y):


        self.logger_object.log(self.file_object,'Entered the get_best_params_for_knn method of the Model_Finder class')
        self.train_x=train_x
        self.train_y=train_y
        try:

            # creating the KNeighborsClassifier model
            self.knn = KNeighborsClassifier()

            # training the KNeighborsClassifier model
            self.knn.fit(self.train_x, self.train_y)
            self.logger_object.log(self.file_object, '. Entered the get_best_params_for_knn method of the Model_Finder class')


            return self.knn

        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in get_best_params_for_knn method of the Model_Finder class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,'KNN ALGORITHM failed. Exited the get_best_params_for_knn method of the Model_Finder class')
            raise Exception()

    def get_best_params_for_naive_bayes(self,train_x,train_y):

        self.logger_object.log(self.file_object,'Entered the get_best_params_for_naive_bayes method of the Model_Finder class')
        self.train_x=train_x
        self.train_y=train_y

        try:
            # initializing with different combination of parameters
            self.param_grid = {"var_smoothing": [1e-9, 0.1, 0.001, 0.5, 0.05, 0.01, 1e-8, 1e-7, 1e-6, 1e-10, 1e-11]}

            # Creating an object of the Grid Search class
            self.grid = GridSearchCV(estimator=self.gnb, param_grid=self.param_grid, cv=3, verbose=3)
            # finding the best parameters
            self.grid.fit(self.train_x, self.train_y)

            # extracting the best parameters
            self.var_smoothing = self.grid.best_params_['var_smoothing']

            # creating a GaussianNB model with the best parameters
            self.gnb = GaussianNB(var_smoothing=self.var_smoothing)
            # training the GaussianNB model
            self.gnb.fit(self.train_x, self.train_y)
            self.logger_object.log(self.file_object,'Naive Bayes best params: ' + str(self.grid.best_params_) + '. Exited the get_best_params_for_naive_bayes method of the Model_Finder class')

            return self.gnb
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in get_best_params_for_naive_bayes method of the Model_Finder class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,'Naive Bayes Parameter tuning  failed. Exited the get_best_params_for_naive_bayes method of the Model_Finder class')
            raise Exception()

    def get_best_params_for_xgboost(self,train_x,train_y):

        self.logger_object.log(self.file_object,'Entered the get_best_params_for_xgboost method of the Model_Finder class')
        self.train_x=train_x
        self.train_y=train_y

        try:
            # initializing with different combination of parameters
            self.param_grid_xgboost = {

                "n_estimators": [50, 100, 130],
                "max_depth": range(3, 11, 1),
                "random_state": [0, 50, 100]

            }
            # Creating an object of the Grid Search class
            self.grid = GridSearchCV(XGBClassifier(objective='binary:logistic'), self.param_grid_xgboost, verbose=3,
                                     cv=2, n_jobs=-1)
            # finding the best parameters
            self.grid.fit(self.train_x, self.train_y)

            # extracting the best parameters
            self.random_state = self.grid.best_params_['random_state']
            self.max_depth = self.grid.best_params_['max_depth']
            self.n_estimators = self.grid.best_params_['n_estimators']

            # creating a  XGBClassifier model with the best parameters
            self.xgb = XGBClassifier(random_state=self.random_state, max_depth=self.max_depth,
                                     n_estimators=self.n_estimators, n_jobs=-1)
            # training the XGBClassifier model
            self.xgb.fit(self.train_x, self.train_y)
            self.logger_object.log(self.file_object,'XGBoost best params: ' + str( self.grid.best_params_) + '. Exited the get_best_params_for_xgboost method of the Model_Finder class')
            return self.xgb

        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in get_best_params_for_xgboost method of the Model_Finder class. Exception message:  ' + str(e))

            self.logger_object.log(self.file_object,'XGBoost Parameter tuning  failed. Exited the get_best_params_for_xgboost method of the Model_Finder class')
            raise Exception()

    def get_best_params_for_random_forest(self,train_x,train_y):
        self.logger_object.log(self.file_object,'Entered the get_best_params_for_random_forest method of the Model_Finder class')
        self.train_x=train_x
        self.train_y=train_y

        try:
            # creating RandomForestClassifier model
            self.rf = RandomForestClassifier()
            # training the RandomForestClassifier model
            self.rf.fit(self.train_x, self.train_y)
            self.logger_object.log(self.file_object,'. Exited the get_best_params_for_random_forest method of the Model_Finder class')

            return self.rf

        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in get_best_params_for_random_forest method of the Model_Finder class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,'RANDOM FOREST failed. Exited the get_best_params_for_random_forest method of the Model_Finder class')
            raise Exception()

    def get_best_params_for_support_vector_machine(self,train_x,train_y):
        self.logger_object.log(self.file_object,'Entered the get_best_params_for_support_vector_machine method of the Model_Finder class')
        self.train_x=train_x
        self.train_y=train_y

        try:
            # creating the support vector machine model
            self.svm = SVC()
            # training the support vector machine model
            self.svm.fit(self.train_x, self.train_y)
            self.logger_object.log(self.file_object,'. Exited the get_best_params_for_support_vector_machine method of the Model_Finder class')

            return self.svm
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in get_best_params_for_support_vector_machine method of the Model_Finder class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,'SVM failed. Exited the get_best_params_for_support_vector_machine method of the Model_Finder class')
            raise Exception()

    def get_best_model(self,train_x,train_y,test_x,test_y):

        self.logger_object.log(self.file_object,'Entered the get_best_model method of the Model_Finder class')
        self.train_x=train_x
        self.test_x=test_x
        self.train_y=train_y
        self.test_y=test_y


        try:

            # create a new list to find the best algorithm
            self.l = []

            # create best model for knn
            self.KNearestNeighbour = self.get_best_params_for_knn(self.train_x,self.train_y)
            self.prediction_KNN = self.KNearestNeighbour.predict(self.test_x)# Predictions using the KNN Model


            if len(self.test_y.unique()) == 1:  # if there is only one label in y, then roc_auc_score returns error. We will use accuracy in that case
                self.KNN_score = accuracy_score(self.test_y, self.prediction_KNN)
                self.logger_object.log(self.file_object, 'Accuracy for KNN:' + str(self.KNN_score))
            else:
                self.KNN_score = roc_auc_score(self.test_y, self.prediction_KNN)  # AUC for KNN
                self.logger_object.log(self.file_object, 'AUC for KNN:' + str(self.KNN_score))

            self.l.append([self.KNearestNeighbour, self.KNN_score, 'knn'])

            # create best model for Logistic Regression
            self.Logistic_Regression = self.get_best_params_for_logistic_regression(self.train_x,self.train_y)
            self.prediction_LOR = self.Logistic_Regression.predict(self.test_x)  # prediction using the Logistic Regression Algorithm



            if len(self.test_y.unique()) == 1:
                # if there is only one label in y, then roc_auc_score returns error. We will use accuracy in that case
                self.LOR_score = accuracy_score(self.test_y, self.prediction_LOR)
                self.logger_object.log(self.file_object, 'Accuracy for LOGISTIC REGRESSION:' + str(self.LOR_score))
            else:
                self.LOR_score = roc_auc_score(self.test_y, self.prediction_LOR)# AUC for LOR
                self.logger_object.log(self.file_object, 'AUC for LOGISTIC REGRESSION:' + str(self.LOR_score))

            self.l.append([self.Logistic_Regression, self.LOR_score, 'logistic_regression'])

            #create best model for Random Forest
            self.RandomForest = self.get_best_params_for_random_forest(self.train_x,self.train_y)
            self.prediction_RF = self.RandomForest.predict(self.test_x)# Predictions using the Random Forest Model



            if len(self.test_y.unique()) == 1:  # if there is only one label in y, then roc_auc_score returns error. We will use accuracy in that case
                self.RF_score = accuracy_score(self.test_y, self.prediction_RF)
                self.logger_object.log(self.file_object, 'Accuracy for RANDOM FOREST:' + str(self.RF_score))
            else:
                self.RF_score = roc_auc_score(self.test_y, self.prediction_RF)#AUC for Random Forest
                self.logger_object.log(self.file_object, 'AUC for RANDOM FOREST:' + str(self.RF_score))

            self.l.append([self.RandomForest, self.RF_score, 'random_forest'])
            # create best model for xgboost
            self.xgboost = self.get_best_params_for_xgboost(self.train_x,self.train_y)
            self.prediction_xgboost = self.xgboost.predict(self.test_x)  # Predictions using the XGBoost Model



            if len(self.test_y.unique()) == 1:  # if there is only one label in y, then roc_auc_score returns error. We will use accuracy in that case
                self.xgboost_score = accuracy_score(self.test_y, self.prediction_xgboost)
                self.logger_object.log(self.file_object, 'Accuracy for XGBoost:' + str(self.xgboost_score))
            else:
                self.xgboost_score = roc_auc_score(self.test_y, self.prediction_xgboost)  # AUC for XGBoost
                self.logger_object.log(self.file_object, 'AUC for XGBoost:' + str(self.xgboost_score))

            self.l.append([self.xgboost, self.xgboost_score, 'xgboost'])
            # create best model for Naive Bayes
            self.naive_bayes = self.get_best_params_for_naive_bayes(self.train_x,self.train_y)
            self.prediction_naive_bayes = self.naive_bayes.predict(self.test_x)  # prediction using the Naive Bayes Algorithm



            if len(self.test_y.unique()) == 1:  # if there is only one label in y, then roc_auc_score returns error. We will use accuracy in that case
                self.naive_bayes_score = accuracy_score(self.test_y, self.prediction_naive_bayes)
                self.logger_object.log(self.file_object, 'Accuracy for NAIVE BIAS:' + str(self.naive_bayes_score))
            else:
                self.naive_bayes_score = roc_auc_score(self.test_y, self.prediction_naive_bayes)  # AUC for Naive Bayes
                self.logger_object.log(self.file_object, 'AUC for NAIVE BIAS:' + str(self.naive_bayes_score))

            self.l.append([self.naive_bayes, self.naive_bayes_score, 'naive_bayes'])
            # create best model for Support Vector Machine
            self.Supportvector = self.get_best_params_for_support_vector_machine(self.train_x,self.train_y)
            self.prediction_SVM = self.Supportvector.predict(self.test_x)# Predictions using the SVM Model


            if len(self.test_y.unique()) == 1:  # if there is only one label in y, then roc_auc_score returns error. We will use accuracy in that case
                self.SVM_score = accuracy_score(self.test_y, self.prediction_SVM)
                self.logger_object.log(self.file_object, 'Accuracy for SUPPORT VECTOR MACHINE:' + str(self.SVM_score))
            else:
                self.SVM_score = roc_auc_score(self.test_y, self.prediction_SVM)  # AUC for SVM
                self.logger_object.log(self.file_object, 'AUC for SUPPORT VECTOR MACHINE:' + str(self.SVM_score))

            self.l.append([self.Supportvector, self.SVM_score])

            self.l = sorted(self.l, key=lambda a: (a[1], a[0]), reverse=True)#sorting the list l in descending order to find the best accuracy model
            self.g = self.l[0]
            return self.g[0], self.g[2] #Extracting the best model abd model name

        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in get_best_model method of the Model_Finder class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,'Model Selection Failed. Exited the get_best_model method of the Model_Finder class')
            raise Exception()