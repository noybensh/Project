from  Selection.Read_excel import Read_excel
from Pre_processing.Pre_processing import *
from Algorithms.Algorithms import *
import json
import time
import pickle


def save_obj(obj, name):
        with open('C:\Users\User\Documents\patients_for_dtw' + '.pkl', 'wb') as f:
            pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

def load_obj(name):
        with open('C:\Users\User\Documents\patients_for_dtw' + '.pkl', 'rb') as f:
            return pickle.load(f)

class Main:
    start_time = time.time()
    # set configurations
    with open('configuration.json') as json_data_file:
        data = json.load(json_data_file)

    value_to_state_df=Read_excel(r'C:\Users\User\Documents\value_state.csv').get_data_frame()
    astract_data = Read_excel('C:\Users\User\Documents\sample.csv').get_data_frame()
    astract_data = Read_excel('C:\Users\User\Documents\AbstractedData.csv').get_data_frame()
    features_threshold = data['Pre_processing']['features_threshold']
    pre_processing = Pre_processing()
    pre_processing.set_value_to_state_dictionary(value_to_state_df)
    relevant_features = pre_processing.get_relevant_features(astract_data,features_threshold)
    patients_after_process = pre_processing.handle_missing_data(relevant_features,astract_data)
    patients_for_dtw=dict()

    for patient in patients_after_process:
        patient_for_dtw = pre_processing.patient_for_dtw(patient)
        patient_id=patient['PatientID'].values[0]
        patients_for_dtw[patient_id] =patient_for_dtw
        print('DTW',patient_id)
    ###for key, value in patients_for_dtw.items():
    ###    print('\t' * 0 + str(key))
    ###    print('\t' *  1 + str(value))
    #for item in patients_for_dtw[patient_id]:
    #   print(item)
    save_obj(patients_for_dtw,'patients_for_dtw')
    ###print('pateins ',len(patients_after_process))
    ###patients_for_dtw = load_obj('')

    ##iDTW = Algorithms.iDTW()

    print("--- %s seconds ---" % (time.time() - start_time))
