import numpy as np;
import pandas as pd;
import matplotlib.pyplot as plt;
import seaborn as sns;
import os;
import json;



class NYCRequestDframeOp():
    
    def readCSVDatatoDataFrame(self):
        '''This method Reads the CSV file and store into Data Frame'''
        df_csv_data_311_resquests_nyc = pd.DataFrame();
        try:
          if(os.path.exists('/content/drive/My Drive/Python_Midterm_Files/311_Service_Requests.csv')):
            df_csv_data_311_resquests_nyc = pd.read_csv('/content/drive/My Drive/Python_Midterm_Files/311_Service_Requests.csv');
          else:
            print("311_Service_Requests.csv file not found");
        except FileNotFoundError:
          print('File not found Error!!:: 311_Service_Requests.csv');
        except Exception:
          print('Error occured reading  311_Service_Requests.csv File!!');
        return df_csv_data_311_resquests_nyc;
        
    def readJSONDatatoDataFrame(self):
        '''This method Reads the JSON file and store into Data Frame'''
        df_json_data_311_resquests_nyc = pd.DataFrame();
        if(os.path.exists('/content/drive/My Drive/Python_Midterm_Files/311_Service_Request_json_format_data.json')):
            json_data_311_resquests_nyc = json.load(open('/content/drive/My Drive/Python_Midterm_Files/311_Service_Request_json_format_data.json'));
            df_json_data_311_resquests_nyc = pd.DataFrame(json_data_311_resquests_nyc["data"]);
        else:
            print("311_Service_Request_json_format_data.json file not found");
        return df_json_data_311_resquests_nyc;
        
    def sliceRequiredDataFromDFrame(self, df_json_data_311_resquests_nyc):
        '''This method Reads Slices Data Frame'''
        df_json_data_311_resquests_nyc = df_json_data_311_resquests_nyc.iloc[:, 8:];
        return df_json_data_311_resquests_nyc;
    
    def renameDataFrameColNames(self, df_json_data_311_resquests_nyc):
        '''This method Reads Slices Data Frame'''
        df_json_data_311_resquests_nyc = df_json_data_311_resquests_nyc.rename(columns={8:'Unique Key' ,9:'Created Date' ,10:'Closed Date' ,11:'Agency' ,12:'Agency Name' ,13:'Complaint Type' ,14:'Descriptor' ,15:'Location Type' ,16:'Incident Zip' ,17:'Incident Address' ,18:'Street Name' ,19:'Cross Street 1' ,20:'Cross Street 2' ,21:'Intersection Street 1' ,22:'Intersection Street 2' ,23:'Address Type' ,24:'City' ,25:'Landmark' ,26:'Facility Type' ,27:'Status' ,28:'Due Date' ,29:'Resolution Description' ,30:'Resolution Action Updated Date' ,31:'Community Board' ,32:'BBL' ,33:'Borough' ,34:'X Coordinate (State Plane)' ,35:'Y Coordinate (State Plane)' ,36:'Open Data Channel Type' ,37:'Park Facility Name' ,38:'Park Borough' ,39:'Vehicle Type' ,40:'Taxi Company Borough' ,41:'Taxi Pick Up Location' ,42:'Bridge Highway Name', 43:'Bridge Highway Direction', 44:'Road Ramp', 45:'Bridge Highway Segment', 46:'Latitude', 47:'Longitude', 48:'Location'})
        return df_json_data_311_resquests_nyc;
        
    def dropUnnecessarlyColumns(self, df_311_resquests_nyc):
        '''This function is used to drop unnecessary Columns from the data frame'''
        columnNames = ['Descriptor', 'Location', 'Facility Type', 'Agency', 'Community Board', 'Park Facility Name', 'Park Borough', 'X Coordinate (State Plane)', 'Y Coordinate (State Plane)','Bridge Highway Name','Bridge Highway Direction','Bridge Highway Segment', 'Taxi Pick Up Location','Landmark','Due Date'];
        df_311_resquests_nyc.drop(columnNames, axis=1, inplace=True);
        return df_311_resquests_nyc;
        
        
    def dropEmptyColumns(self, df_311_resquests_nyc):
        '''This function is used to drp the almost/complete empty columns'''
        naColumnsWithCount = df_311_resquests_nyc.isna().sum().where(lambda x:x>0).dropna();
        naColumnsNames = [];
        for naColumns in naColumnsWithCount.iteritems():
            if(naColumns[1]/2678000 >= 1.0):
                naColumnsNames.append(naColumns[0]);
        df_311_resquests_nyc.drop(naColumnsNames, axis=1, inplace=True);
        return df_311_resquests_nyc;
         
         
    def convertToLowerCase(self, df_311_resquests_nyc):
        '''This function is used to convert all the upper letters to lower case letters'''
        df_311_resquests_nyc.applymap(lambda charVal :  charVal.lower() if type(charVal) == str else  charVal);
        return df_311_resquests_nyc;
        
    def displayEmptyColCount(self, df_311_resquests_nyc):
        '''This function displayes the empty column count'''
        print(df_311_resquests_nyc.isna().sum().where(lambda x:x>0).dropna());
        
    def convertStrToDateTime(self, strDateValue, df_311_resquests_nyc):
        '''This function converts the date from String to Date Time type'''
        df_311_resquests_nyc[strDateValue] = pd.to_datetime(df_311_resquests_nyc[strDateValue]);
        return df_311_resquests_nyc;
        
    def createResolutionTimeCol(self, df_311_resquests_nyc):
        '''This function creates a new column Request Resolution Time'''
        df_311_resquests_nyc['Request Resolution Time'] = df_311_resquests_nyc['Closed Date'].values - df_311_resquests_nyc['Created Date'].values;
        return df_311_resquests_nyc;
        
    def createResolTimeColInMin(self, df_311_resquests_nyc):
        '''This function creates a new column Request Resolution Time in Minutes'''
        df_311_resquests_nyc['Request Resolution Time in Min'] = df_311_resquests_nyc['Request Resolution Time'] / np.timedelta64(1,'m');
        return df_311_resquests_nyc;
        
    def createMonthColFromCloseDate(self, df_311_resquests_nyc):
        '''This function created new column Request Closing Month '''
        df_311_resquests_nyc['Request Closing Month'] = df_311_resquests_nyc['Closed Date'].dt.month;
        return df_311_resquests_nyc;
        
    def updateColswithModeValue(self, df_311_resquests_nyc):
        '''This function updated the City, Incident Zip, Latitude and Longitude with the corresponging Highest Occuring(mode) value'''
        city = df_311_resquests_nyc.City.mode()[0];
        zipcode = df_311_resquests_nyc.loc[df_311_resquests_nyc.City == city, 'Incident Zip'].mode()[0];
        latitude = df_311_resquests_nyc.loc[df_311_resquests_nyc['Incident Zip'] == zipcode, 'Latitude'].mode()[0];
        longitude = df_311_resquests_nyc.loc[df_311_resquests_nyc['Incident Zip'] == zipcode, 'Longitude'].mode()[0];
        
        df_311_resquests_nyc.loc[df_311_resquests_nyc.City.isna(), 'Incident Zip'] = zipcode;
        df_311_resquests_nyc.loc[df_311_resquests_nyc.City.isna(), 'City'] = city;
        df_311_resquests_nyc.loc[((df_311_resquests_nyc.City == city) &  (df_311_resquests_nyc.Latitude.isna() == True)), 'Latitude'] = latitude;
        df_311_resquests_nyc.loc[((df_311_resquests_nyc.City == city) &  (df_311_resquests_nyc.Longitude.isna() == True)), 'Longitude'] = longitude;
        return df_311_resquests_nyc;
        
    def dropNaRecords(self, df_311_resquests_nyc):
         '''This function drops the records with empty Values'''
         df_311_resquests_nyc.dropna(inplace=True);
    
class NYCRequestDataPlot():
    def drawHistogramPlot(self, df_311_resquests_nyc):
        '''This function displays a Histogram Plot of Request Resolution Time in Min'''
        plt.figure(figsize=(18,8));
        df_311_resquests_nyc[df_311_resquests_nyc['Borough'] == 'BROOKLYN']['Request Resolution Time in Min'].plot.hist(range=(0,1500));
        plt.title("Request Resolution Time in Min vs Count");
        plt.xlabel("Request Resolution Time in Min");
        
    def drawBarPlotCityComplaintType(self, df_311_resquests_nyc):
        '''This function displays a Bar graph between City and Complaint'''
        df_graph_data=df_311_resquests_nyc.groupby(['City','Complaint Type']).size().unstack().fillna(0);
        df_graph_data.plot.bar(figsize=(15,10), stacked=True);
        plt.legend(bbox_to_anchor=(1.05, 1), ncol=3);
        plt.ylabel('Number of Complaints');
        plt.title('Number of complaints VS City');
        
    def getNoOfCasesByCityStatus(self, df_311_resquests_nyc):
        '''This function displayes The Number of Cases by City and Status'''
        df_city_status=df_311_resquests_nyc.groupby(['City','Status']).size().unstack().fillna(0)
        df_city_status.sort_values(by='Open', ascending=False).head();
        
    def getUnresovedCasesPercentage(self):
        '''This function displayes The Unresolved Cases Percentage'''
        df_city_status=df_311_resquests_nyc.groupby(['City','Status']).size().unstack().fillna(0);
        df_city_status['Unresolved_percentage']= df_city_status['Open']/df2['Open'].sum()*100;
        df_city_status.sort_values(by='Unresolved_percentage', ascending=False).head().sum();
        
        
    def drawCountPlotComplaintType(self, df_311_resquests_nyc):
        '''This function displays a Count Plot for BROOKLYN City's Complaint Type'''
        df_311_resquests_nyc.loc[(df_311_resquests_nyc['City']=='BROOKLYN'),:]['Complaint Type'].value_counts();
        plt.figure(figsize=(18,5));
        df_count_plot= sns.countplot(x=df_311_resquests_nyc.loc[df_311_resquests_nyc.City=='BROOKLYN']['Complaint Type'], palette='YlOrRd_r');
        df_count_plot.set_xticklabels(df_count_plot.get_xticklabels(), rotation=90);
        plt.title('Complaint Type Vs Count');
      
    def drawBarPlotAgencyComplaintType(self, df_311_resquests_nyc): 
        '''This function displays a bar graph for Agency Name and Complaint Type'''
        df_bar_plot=df_311_resquests_nyc.groupby(['Agency Name','Complaint Type']).size().unstack().fillna(0);
        df_bar_plot.plot.bar(figsize=(15,8), stacked=True);
        plt.legend(bbox_to_anchor=(1.05, 1), ncol=3);
        plt.ylabel('Number of Complaints');
        plt.title('Number of complaints VS Agency Name');
        
    def drawCountPlotLocationType(self, df_311_resquests_nyc):
        '''This function displayes count plot for Location Type VS Count'''
        plt.figure(figsize=(19,5));
        count_plot =sns.countplot(df_311_resquests_nyc['Location Type']);
        count_plot.set_xticklabels(count_plot.get_xticklabels(), rotation=90);
        plt.title('Location Type Vs Count');
        
    def drawBarPlotBoroughClosingTimeMin(self, df_311_resquests_nyc):
        '''This function displays a bar graph for Borough and Request_Closing_Time_mins'''
        plt.figure(figsize=(8,7));
        sns.barplot(x='Borough', y='Request Resolution Time in Min', data=df_311_resquests_nyc);
        plt.title('Average Request Closing Time for Boroughs');
    
    def drawBarPlotComplaintRequestClosingTime(self, df_311_resquests_nyc):
        '''This function displays a bar graph for Agency Name and Complaint Type'''
        df_311_resquests_nyc['Complaint Type']=df_311_resquests_nyc.index;
        plt.figure(figsize=(10,5));
        sns.barplot(x='Complaint Type', y='Request Resolution Time in Min', data=df_311_resquests_nyc.sort_values('Request Resolution Time in Min'));
        plt.xticks(rotation=90);
        
        
    def drawPairPlot(self, df_311_resquests_nyc):
        '''This function displays a Pair Plot'''
        sns.pairplot(df_311_resquests_nyc.head(100000));
        
    def drawHeatMap(self, df_311_resquests_nyc):
        '''This function displays a HeatMap Plot'''
        plt.figure(figsize=(15,5));
        columns_aa_pt = df_311_resquests_nyc.pivot_table(index='Request Closing Month', columns=['Borough','Complaint Type']);
        sns.heatmap(columns_aa_pt);
        
    def drawHeatMapCorr(self, df_311_resquests_nyc):
        '''This function displays a HeatMap Plot with Correlation value'''
        plt.figure(figsize=(15,5));
        columns_aa_pt = df_311_resquests_nyc.corr()
        sns.heatmap(columns_aa_pt, annot=True, cmap='coolwarm', linecolor = 'black', linewidths=1 );
        plt.title('Heatmap Correlation');
        
    def drawScatterPlot(self, df_311_resquests_nyc):
        '''This function displays a Scatter Plot of Request Resolution Time in Min and Complaint Type'''
        df_311_resquests_nyc.plot.scatter(x='Request Closing Month', y='Complaint Type', figsize=(20,18));
        plt.title("Complaint Type VS Request Resolution Time in Min");
        plt.xlabel("Request Resolution Time in Min");
        plt.ylabel("Complaint Type");
        
    def createCategoricalColumns(self, df_311_resquests_nyc):
        '''This function creates categorical columns for Complaint Type, City and Borough columns'''
        df_311_resquests_nyc['Complaint Type Code']=df_311_resquests_nyc['Complaint Type'].astype('category').cat.codes;
        df_311_resquests_nyc['City Code']= df_311_resquests_nyc['City'].astype('category').cat.codes;
        df_311_resquests_nyc['Borough Code']= df_311_resquests_nyc['Borough'].astype('category').cat.codes;
        return df_311_resquests_nyc;
        
    def filterDataBasedCategory(self, df_311_resquests_nyc, categoryName, *categoryValArg, **DispColumnsKArg):
        '''This function creates categorical columns for Complaint Type, City and Borough columns'''
        DisplayColumns = [DispColumnsKArg['City_Code'], DispColumnsKArg['Borough_Code'],DispColumnsKArg['Complaint_Type_Code']];
        df_311_req_filtered_data = df_311_resquests_nyc[df_311_resquests_nyc[categoryName].isin(list(categoryValArg))][DisplayColumns];
        return df_311_req_filtered_data;
        
    def covertDataFrToMatrix(self, df_311_req_filtered_data):
        '''This function Converts Data Frame into Matrix format'''    
        df_311_req_matrix = np.asmatrix(df_311_req_filtered_data);
        return df_311_req_matrix;
        
    def covertMatrixToArray(self, df_311_req_matrix):
        '''This function Converts Data Frame into Matrix format'''    
        df_311_req_array = np.array(df_311_req_matrix);
        return df_311_req_array;
        
    def updloadDataToNumpy(self, df_311_resquests_nyc):
        '''This function Uploads Data Frame to Numpy Array'''    
        numPyarray_311_req = df_311_resquests_nyc.to_numpy();
        return numPyarray_311_req;

    def sliceAndDisplayData(self, numPyarray_311_req):
        '''This function Slice the first 10 Rows, 2nd, 3rd and 4th column data'''    
        numPyarray_311_req_slice_data = numPyarray_311_req[0:10, 1:4];
        return numPyarray_311_req_slice_data;
        
    def filterDataBasedOnAgencyNameTimeResCity(self, df_311_resquests_nyc):
        '''This function Filters data based on the Agency name, Request Resolution Time in Min and City Code(6=BROOKLYN)'''    
        df_311_resquests_nyc_filter_data = df_311_resquests_nyc.loc[(df_311_resquests_nyc['Agency Name'] == 'New York City Police Department') & (df_311_resquests_nyc['City Code'] == 5) & (df_311_resquests_nyc['Request Resolution Time in Min'] <= 10)]
        return df_311_resquests_nyc_filter_data;
        
    def getAverageResolvedTimeInMin(self, df_311_resquests_nyc):
        '''This function returns the average Time in minutes to resolve the 311 NYC Request Issues'''
        total_reolved_time_in_min= df_311_resquests_nyc['Request Resolution Time in Min'].sum()
        total_req_count = len(df_311_resquests_nyc.index) - 1
        avg_reolved_time_in_min = total_reolved_time_in_min / total_req_count
        print('Average Time takes to Resolved the 311 NYC issues is: {}'.format(avg_reolved_time_in_min));
