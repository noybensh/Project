import pandas as pd
from collections import deque
import threading
import time
from datetime import datetime,timedelta
from  Selection.Read_excel import Read_excel
import numpy as np

from dateutil import parser

class Pre_processing:
    patients = 0
    patients_to_process = 0
    patients_after_process = 0
    raw_data = 0
    rows_to_insert = 0
    relevant_features_set_with_out_gradient=0
    value_to_state =0
    def set_value_to_state_dictionary(self,value_to_state_dictionary_df):
        global value_to_state
        value_to_state=dict()
        for index, row in value_to_state_dictionary_df.iterrows():
            value_to_state[str.lower(row['feature'])]=[row['start normal value'],row['end normal value']]
    def date_to_string(self,date):
        try:
            string_date =date.strftime("%d/%m/%Y %H:%M:%S")
        except:
            return date
        return string_date
    def patient_for_dtw(self,pateint):
        dtw_patient=list()
        is_sepsis =bool( pateint.loc[pateint['ConceptName'] == 'Sepsis_State','Value'].values[0])
        is_sepsis=False
        if is_sepsis:
            start_time =  pateint.loc[pateint['ConceptName'] == 'Sepsis_State','StartTime'].values[0]
        else:
            patient_first_sample_time = pateint["StartTime"].astype('datetime64').min()
            patient_first_sample_time = min([parser.parse(k, dayfirst=True) for k in pateint["StartTime"]])
            patient_first_sample_time=self.date_to_string(patient_first_sample_time)
            start_time = self.get_time_shift(patient_first_sample_time, 24 * 3)

        for feature in relevant_features_set_with_out_gradient:
            feature_data = pateint.loc[pateint['ConceptName'] == feature]
            if feature_data.empty:
                dtw_patient.append(self.generate_null_list(12))
            else:
                feature_data_list = list()
                for i in range (12):
                    current_value =self.get_value_at_time_stamp(feature_data,start_time)
                    feature_data_list.append(current_value)
                    start_time=self.get_time_shift(self.date_to_string(start_time), 1)
                dtw_patient.append((feature_data_list))
        #for i,row in enumerate(dtw_patient):
        #    try:
        #        dtw_patient[i]=[w.replace('high', '3') for w in dtw_patient[i]]
        #        dtw_patient[i]=[w.replace('normal', '2') for w in dtw_patient[i]]
        #        dtw_patient[i]=[w.replace('low', '1') for w in dtw_patient[i]]
        #        dtw_patient[i]=[w.replace('Fever', '3#') for w in dtw_patient[i]]
        #        dtw_patient[i]=[w.replace('low', '1') for w in dtw_patient[i]]
        #    except:
        #        continue
        return dtw_patient
    def get_value_at_time_stamp(self,df,time_stamp):
        time_stamp=parser.parse(time_stamp, dayfirst=True)
        for index, row in df.iterrows():
            current_start_time =parser.parse(row['StartTime'], dayfirst=True)
            current_end_time =parser.parse(row['EndTime'], dayfirst=True)
            if current_start_time <= time_stamp and time_stamp<=current_end_time:
                return row['Value']
        return -1;
    def generate_null_list(self,n):
        return [-1 for x in range(n)]
    def get_relevant_features(self,df,threshold):
         global patients
         patients = df.groupby('PatientID').nunique()['PatientID']
         patients_amount =patients.count()
         patients_threshold = threshold * patients_amount
         all_concepts_name_and_mount = df.groupby(['PatientID','ConceptName']).size().groupby(['ConceptName']).size().reset_index()
         relevant_features = all_concepts_name_and_mount.loc[all_concepts_name_and_mount[0]>=patients_threshold,{'ConceptName'}]
         relevant_features_set=pd.Series(relevant_features['ConceptName'])
         relevant_features_set= set(relevant_features_set)
         global relevant_features_set_with_out_gradient
         relevant_features_set_with_out_gradient=set()
         for feature in relevant_features_set:
             if 'gradient' not in str.lower(feature):
                 relevant_features_set_with_out_gradient.add(feature)
         return relevant_features_set_with_out_gradient
    def handle_missing_data(self,relevant_features,astract_data):
        global patients_to_process
        global patients_after_process
        global raw_data
        global rows_to_insert
        rows_to_insert=list()
        #raw_data = Read_excel('C:\Users\User\Documents\w_sample.csv').get_data_frame()
        raw_data = Read_excel('C:\Users\User\Documents\RawData.csv').get_data_frame()
        patients_after_process = list()
        patients_to_process  = deque()
        test=0
        # remove un relevant features
        astract_data = astract_data.loc[astract_data['ConceptName'].isin(relevant_features)]
        astract_data.loc[astract_data['Value']=='high','Value']='3'
        astract_data.loc[astract_data['Value']=='Fever','Value']='3'
        astract_data.loc[astract_data['Value']=='Normal','Value']='2'
        astract_data.loc[astract_data['Value']=='normal','Value']='2'
        astract_data.loc[astract_data['Value']=='low','Value']='1'
        astract_data.loc[astract_data['Value']=='Hypotermia','Value']='1'

        for patient in patients.index.values:
            print('patient append to list',patient)
            patients_to_process.append(astract_data.loc[astract_data['PatientID'] ==patient])
        number_if_threads=1
        for i in range(number_if_threads):
            threading.Thread(target=self._func_to_be_threaded).start()
        while(len(patients_after_process)<len(patients)):
            time.sleep(1)
        return patients_after_process
    def _func_to_be_threaded(self):
        while len(patients_to_process) > 0:
            try:
                patient = patients_to_process.pop()
                patients_after_process.append(self.fill_patient_gap(patient))
                patient_id = patient['PatientID'].values[0]
                print('PROCESS', patient_id)
            except:
                return
    def fill_patient_gap(self,patient):

        patinet_first_index=int(patient.iloc[0].name)
        if patient['PatientID'].values[0]==10004:
            patinet_first_index=0
        prev_row = 0
        for index, row in patient.iterrows():
            if index == 986:
                a = 5
            if index == patinet_first_index:
                prev_row = row
                continue
            if prev_row['ConceptName'] !=row['ConceptName']:
                prev_row = row
                continue

            gap_size = self.get_gap_size_hours(row['StartTime'],prev_row['EndTime'])
            if gap_size<=24 and gap_size >0:
                prev_duration = self.get_gap_size_hours(prev_row['EndTime'], prev_row['StartTime'])
                current_duration = self.get_gap_size_hours(row['EndTime'], row['StartTime'])
                if gap_size<=1:
                    temp_row = row
                    if current_duration>=prev_duration:
                        patient.at[index, 'StartTime'] = prev_row['EndTime']
                    else:
                        patient.at[index-1, 'EndTime'] = row['StartTime']
                else:
                    min = float(1.0/60)

                    if prev_duration==0 :
                        prev_duration= float(min)
                    if current_duration == 0:
                        current_duration = float(min)

                    ratio= prev_duration/(current_duration+prev_duration)
                    new_end_time=self.get_time_shift(prev_row['EndTime'],gap_size*ratio)
                    patient.at[index-1, 'EndTime'] = new_end_time
                    patient.at[index , 'StartTime'] = new_end_time

            if gap_size>24:
                patient_id=row['PatientID']
                concept = row['ConceptName']
                start_time = prev_row['EndTime']
                end_time = row['StartTime']
                value = self.get_patient_state_average_value(patient_id,concept)
                patient.at[index, 'Value'] =self.value_to_state(concept,value)
                rows_to_insert.append(row)
            prev_row = row
        return patient
    def value_to_state(self,concept,value):
        concept=str.lower(concept)
        concept=str.replace(concept,'-state','')
        if concept not in value_to_state:
            return '-1'
        if value_to_state[concept][0]>value:
            return '1'
        elif value_to_state[concept][1]<value:
            return '3'
        return '2'
    def get_patient_state_average_value(self,patient,concept):
        concept=str.replace(concept,'-State','')
        patient_raw_data = raw_data.loc[raw_data['PatientID']==patient,{'Value','ConceptName'}]
        patient_concept_raw_data=patient_raw_data.loc[patient_raw_data['ConceptName']==concept,{'Value'}]
        if patient_concept_raw_data.empty:
            concept=concept+'-BLOOD'
            patient_concept_raw_data = patient_raw_data.loc[patient_raw_data['ConceptName'] == concept, {'Value'}]
        patient_concept_raw_data["Value"] =patient_concept_raw_data["Value"].astype('float64')

        return patient_concept_raw_data["Value"].mean()
    def get_time_shift(self,date,hours):
        date= parser.parse(date,dayfirst=True)
        date=date + timedelta(hours=hours)
        #t = self.date_to_string(date)
        return self.date_to_string(date)
    def get_gap_size_hours(self,start_time,end_time):
        # retrive data gap time, for format  DD/MM/YYYY HH:mm exmaple 10/08/2192 22:29
        start_time = parser.parse(start_time,dayfirst=True)
        end_time = parser.parse(end_time,dayfirst=True)
        time_diffs= start_time-end_time
        if str(time_diffs)[:1] == '-':
            return 0
        time_diffs_seconds=float(time_diffs.total_seconds())
        time_diffs_houres=time_diffs_seconds/3600
        return time_diffs_houres
