import pandas as pd
class Read_excel:
    # constructor
    def __init__(self,path): 
        self.path=path
        
    def get_data_frame(self):
        return pd.read_csv(self.path, low_memory=False)


 #  height = data.loc[data['ConceptName']=='Sepsis_State',{'Value','PatientID'}]#.values[1]

