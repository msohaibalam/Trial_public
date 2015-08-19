import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import heapq
import pymysql as mdb

print "Reading in file"

pd.set_option('display.max_columns',60)
df1 = pd.read_csv('/Users/msohaibalam/Documents/Insight/ValorWaterAnalytics/data/Utility1.txt')

##########################
##### Anomaly 1 ##########
### (MeterRightSizing) ###
##########################

print "Working on Anomaly 1..."

hash_base_charge = {0.62:9.00,
                    0.75:9.00,
                    1.0:14.00,
                    1.5:24.00,
                    2.0:39.00,
                    3.0:70.00,
                    4.0:115.00,
                    6.0:225.00,
                    8.0:360.00}

# 0.75 and values beyond 3.0 proposed using linear fit
hash_max_volume = {0.62:720*20,
                      0.75:720*30,
                      1.0:720*50,
                      1.5:720*100,
                      2.0:720*160,
                      3.0:720*300,
                      4.0:720*450,
                      6.0:720*750,
                      8.0:720*1050}
                      
df1['base_charge'] = None
df1['max_totvolume'] = None

# Water meters
for i in df1.loc[(df1.water==1)&(df1.meter_water.notnull()==True)].index:
    df1.base_charge[i] = hash_base_charge.get(df1.meter_water[i])
    df1.max_totvolume[i] = hash_max_volume.get(df1.meter_water[i])
    
# Irrigation meters
for i in df1.loc[(df1.irrigation==1)&(df1.meter_irrigation.notnull()==True)].index:
    df1.base_charge[i] = hash_base_charge.get(df1.meter_irrigation[i])
    df1.max_totvolume[i] = hash_max_volume.get(df1.meter_irrigation[i])
    
df_maxvolnotnull = df1.loc[df1.max_totvolume.notnull()==True]
df1['anomaly1'] = None

for i in df_maxvolnotnull.index:
    if df1.totvolume[i]>df1.max_totvolume[i]:
        df1.anomaly1[i] = 1
    else:
        df1.anomaly1[i] = 0
        
df1['anom1_revloss'] = None
df_anom1 = df1[df1.anomaly1==1]

for i in df_anom1.index:
    #print "Total Volume: ", df1.totvolume[i]
    vol_diff = {}
    for key in hash_max_volume.keys():
        #print key, hash_max_volume[key], df1.totvolume[i] - hash_max_volume[key]
        if df1.totvolume[i] - hash_max_volume[key]<0:
            vol_diff[key] = df1.totvolume[i] - hash_max_volume[key]
    #print max(vol_diff, key=vol_diff.get)
    if df1.irrigation[i]==1:
        df1.anom1_revloss[i] = hash_base_charge[max(vol_diff, key=vol_diff.get)] - hash_base_charge[df1.meter_irrigation[i]]
    elif df1.water[i]==1:
        df1.anom1_revloss[i] = hash_base_charge[max(vol_diff, key=vol_diff.get)] - hash_base_charge[df1.meter_water[i]]
        
print "Done working on Anomaly 1!"
print "*" *10
        
##########################
##### Anomaly 2 ##########
## (MissingBaseCharge) ###
##########################

print "Working on Anomaly 2..."

df1_anom2 = df1.loc[(df1.totcharge.notnull()==True)&(df1.base_charge.notnull()==True)]

df1['anomaly2'] = None
df1['anom2_revloss'] = None
for i in df1_anom2.index:
    if df1.totcharge[i]<df1.base_charge[i]:
        df1.anomaly2[i] = 1
        if df1.irrigation[i]==1:
            df1.anom2_revloss[i] = hash_base_charge.get(df1.meter_irrigation[i])
        elif df1.water[i]==1:
            df1.anom2_revloss[i] = hash_base_charge.get(df1.meter_water[i])
    else:
        df1.anomaly2[i] = 0
        
print "Done working on Anomaly 2!"
print "*" *10

##########################
##### Anomaly 3 ##########
## (HiddenIrrigator) #####
##########################

print "Working on Anomaly 3..."

df1_peaking = df1.loc[(df1.meternumber.notnull()==True)&(df1.fy.notnull()==True)]
mygroup = df1_peaking.groupby(['meternumber','fy'])

# calculate peaking
df1['peaking'] = None
for combo, dframe in mygroup:
    NonzeroList = [i for i in dframe.totvolume if i>0]
    if len(NonzeroList)>=6:
        largest3 = heapq.nlargest(3,NonzeroList)
        smallest3 = heapq.nsmallest(3,NonzeroList)
        maxline = np.mean(largest3)
        minline = np.mean(smallest3)
        peak = maxline/float(minline)
        for i in dframe.index:
            df1.peaking[i] = peak

# calculate median peaking of irrigation customers for each FY       
df1_meters = df1.loc[df1.meternumber.notnull()==True]
set_fy = set(df1_meters.fy)
med_irr_peak = {}
for i in set_fy:
    med_irr_peak[i] = df1.loc[(df1.fy==i)&(df1.irrigation==1)&(df1.peaking.notnull()==True)].peaking.median()
    
summer_months = [5,6,7,8,9,0]
df1_summer = df1.loc[(df1.totvolume.notnull()==True)&((df1.period%10).isin(summer_months))]
mygroup2 = df1_summer.groupby(['meternumber','fy'])

# calculate average summer use
df1['avg_summer_use'] = None
for key, dframe in mygroup2:
    my_avg = dframe.totvolume.mean()
    for i in dframe.index:
        df1.avg_summer_use[i] = my_avg

df1['median_peaking_irr_customers'] = [med_irr_peak[df1.fy[i]] for i in df1.index]

#sfdu customers only, people with either zero irrigation use in a given year or no irrigation meter
df_hidirr1 = df1.loc[(df1.SFDU==1)&((df1.irrvolume==0)|(df1.irrigation==0))]

#with peaking >=0.7x median peaking of irrigation customers
df_hidirr2 = df_hidirr1.loc[df_hidirr1.peaking>=0.7*df_hidirr1.median_peaking_irr_customers]

# ^ Check if was comparing against null values
# print len(df_hidirr2.loc[df_hidirr2.median_peaking_irr_customers.notnull()==False])
# Good!

df_hidirr3 = df_hidirr2.loc[(df_hidirr2.avg_summer_use.notnull()==True)&(df_hidirr2.avg_summer_use>=10)]

# Note: anomaly3 is non-missing for all entries in the dataset, unlike previous two anomalies
# If need to focus on subset of entire dataset, could later do so during machine learning work
df1['anomaly3'] = [1 if i in df_hidirr3.index else 0 for i in df1.index]

df_anom3_true = df1.loc[df1.anomaly3==1]

# using Outside Residential Customers, Scenario 4 value for FPWC Total
# Note: following results in no missing values for anom3_revloss
df1['anom3_revloss'] = [1693 if i in df_anom3_true.index else 0 for i in df1.index]

print "Done working on Anomaly 3!"
print "*" *10


###########################
##### Anomaly 5 ###########
# (High Volume Anomalies) #
###########################

print "Working on Anomaly 5..."

# subset of data with residential customers, with non-missing meternumber and total_volume values
df1_spike1 = df1.loc[(df1.residential==1)&(df1.meternumber.notnull()==True)&(df1.totvolume.notnull()==True)]

MyGroupAnom5 = df1_spike1.groupby(['meternumber','fy'])

df1['spike_ratio'] = None
summer_months = [5,6,7,8,9,0]
winter_months = [1,2,3,4] #1 covers both Jan and Nov, 2 both Feb and Dec
for combo, dframe in MyGroupAnom5:
    avg_summer = dframe[(dframe.period%10).isin(summer_months)].totvolume.mean()
    avg_winter = dframe[(dframe.period%10).isin(winter_months)].totvolume.mean() + 0.01 #add a small perturbation term to avoid division by zero
    spiking = avg_summer/float(avg_winter)
    for i in dframe.index:
        df1.spike_ratio[i] = spiking
        
dict_spike_perc = {}
for i in set_fy:
    dict_spike_perc[i] = df1.loc[(df1.fy==i)&(df1.residential==1)&(df1.meternumber.notnull()==True)&(df1.totvolume.notnull()==True)&(df1.spike_ratio.notnull()==True)].spike_ratio.quantile(0.95)

df1['spike_95th_percentile'] = [dict_spike_perc.get(df1.fy[i]) for i in df1.index]

# subset of data with residential customers, with non-missing meternumber, total_volume and spike_ratio values
df1_spike2 = df1.loc[(df1.residential==1)&(df1.meternumber.notnull()==True)&(df1.totvolume.notnull()==True)&(df1.spike_ratio.notnull()==True)]
#len(df_spike2)/float(len(df1))

df1['anomaly5'] = None
for i in df1_spike2.index:
    if ((df1.totvolume[i]>10)&(df1.spike_ratio[i]>df1.spike_95th_percentile[i])):
        df1.anomaly5[i] = 1
    else:
        df1.anomaly5[i] = 0
        
print "Done working on Anomaly 5!"
print "*" *10

###########################
##### Anomaly 4 ###########
# (Irrigation Tampering) ##
###########################
## did not complete this part in time
#####################################

#dict_irr_perc = {}
#for i in set_fy:
#    dict_irr_perc[i] = df1.loc[(df1.fy==i)&(df1.irrigation==1)&(df1.irrvolume>0)].irrvolume.quantile(0.25)

#df1['irr_25th_percentile'] = [dict_irr_perc.get(df1.fy[i]) for i in df1.index]

# Used at least the 25th percentile of irrigation use in previous years
#df1_irr_tamp1 = df1.loc[(df1.irrigation==1)&(df1.irr_25th_percentile.notnull()==True)&(df1.totvolume.notnull()==True)&(df1.totvolume>=df1.irr_25th_percentile)]

# set of meters that used at least the 25th percentile of irrigation use in some month
# only look at the portion of the dataset with non-missing meternumber values
# this is the set of (irrigation) meters that had at least one month where they used at least the 25th percentile of irrigation
#SetMetersIrrTamp1 = set(df1_irr_tamp1.loc[df1_irr_tamp1.meternumber.notnull()==True].meternumber)

# this is the (subset of) dataframe that only includes the meters that had at least one month of irrigation use greater than the 25th percentile in the respective fiscal year
#df1_irr_tamp2 = df1.loc[df1.meternumber.isin(SetMetersIrrTamp1)]

#MyGroupIrrTamp1 = df1_irr_tamp2.groupby(['meternumber','fy'])

#df1['ratio_months_irr_25th_perc'] = None

# calculate ratio of months with >25th percentile use, to total no. of months
#for combo, dframe in MyGroupIrrTamp1:
#    AboveBasePercVols = [dframe.totvolume[i] for i in dframe.index if dframe.totvolume[i]>dframe.irr_25th_percentile[i]]
#    Ratio = len(AboveBasePercVols)/float(len(dframe))
#    for i in dframe.index:
#        df1.ratio_months_irr_25th_perc[i] = Ratio

#df1_irr_tamp3 = df1.loc[(df1.meternumber.notnull()==True)&(df1.ratio_months_irr_25th_perc.notnull()==True)]

#dict_meter_ratio = {}
#for meter in SetMetersIrrTamp1:
#    ratio_list = []
#    for year in set_fy:
#        tmp_set = set(df1_irr_tamp3.loc[(df1_irr_tamp3.meternumber==meter)&(df1_irr_tamp3.fy==year)].ratio_months_irr_25th_perc)
#        tmp_elem_list = list(tmp_set)
#        if tmp_elem_list!=[]:
#            tmp_elem = tmp_elem_list[0]
#            ratio_list.append(tmp_elem)
#    dict_meter_ratio[meter] = ratio_list
    
# This prints the list with beginning ratio 1.0, ending ratio 0.0 (still doesn't imply zero volume usage)
#for key in dict_meter_ratio:
#    #print key, ": ", dict_meter_ratio[key]
#    if ((dict_meter_ratio[key][0]==1.0)and(dict_meter_ratio[key][-1]==0.0)):
#        print key, ": ", dict_meter_ratio[key]
        
## Inspecting only the cases where ratios were either 0.0 or 1.0
#for key in dict_meter_ratio.keys():
#    if set(dict_meter_ratio[key])=={0.0,1.0}:
#        print "MeterNumber: ", key
#        for year in set_fy:
#            print "Year: ", year, ", TotVolume: ", df1.loc[(df1.meternumber==key)&(df1.fy==year)].totvolume.sum()
#        print "*" *10


###############################################
##### New predictions for Anomaly 2 ###########
####### (Misisng Base Charge) #################
###############################################

print "Working on Anomaly 2 New preds..."

df1['anom2_newpreds'] = None
BaseCharges = set(df1.loc[df1.base_charge.notnull()==True].base_charge)

#num_new_preds = {}
NonAnomBaseMean = {}
NonAnomBaseVar = {}
for base in BaseCharges:
    num_preds = 0
    #print "Doing work for Base Charge ", base
    non_anom2_base = np.log(list(df1.loc[(df1.base_charge==base)&(df1.totcharge>=0)&(df1.anomaly2==0)].totcharge + 0.01))
    NonMeanBase = non_anom2_base.mean()
    NonAnomBaseMean[base] = NonMeanBase
    NonVarBase = non_anom2_base.var()
    NonAnomBaseVar[base] = NonVarBase
    #perc25 = df1.loc[(df1.meternumber.notnull()==True)&(df1.base_charge==base)&(df1.totcharge>=0)&(df1.anomaly2==0)&(df1.totvolume.notnull()==True)].totvolume.quantile(0.25)
    for i in df1.loc[(df1.meternumber.notnull()==True)&(df1.base_charge==base)&(df1.totcharge>=0)&(df1.anomaly2==0)].index:
        if np.log(df1.totcharge[i]+0.01)<=NonMeanBase-1.65*np.sqrt(NonVarBase):
            num_preds += 1
            df1.anom2_newpreds[i] = 1
            df1.anom2_revloss[i] = base
    #num_new_preds[base] = num_preds
    #print "New predictions: ", num_new_preds[base]
    #print "*" *10
    
print "Done working on Anomaly 2 New preds!"
print "*" *10


## Use following to create graph

#non_anom2_totchr = np.log(list(df1.loc[(df1.totcharge>=0)&(df1.base_charge>=0)&(df1.anomaly2==0)].totcharge+0.01))
#non_anom2_baschr = np.log(list(df1.loc[(df1.totcharge>=0)&(df1.base_charge>=0)&(df1.anomaly2==0)].base_charge+0.01))

#anom2_totchr = np.log(list(df1.loc[(df1.totcharge>=0)&(df1.base_charge>=0)&(df1.anomaly2==1)].totcharge+0.01))
#anom2_baschr = np.log(list(df1.loc[(df1.totcharge>=0)&(df1.base_charge>=0)&(df1.anomaly2==1)].base_charge+0.01))

#new_anom2_totchr = np.log(list(df1.loc[(df1.totcharge>=0)&(df1.base_charge>=0)&(df1.anom2_newpreds==1)].totcharge+0.01))
#new_anom2_baschr = np.log(list(df1.loc[(df1.totcharge>=0)&(df1.base_charge>=0)&(df1.anom2_newpreds==1)].base_charge+0.01))

#plt.rcParams['figure.figsize'] = (20, 10)
#fig = plt.figure()
#ax = fig.add_subplot(111)

#ax.scatter(non_anom2_totchr, non_anom2_baschr, s=5, c='b', marker="o", label='non-anomalous')
#ax.scatter(anom2_totchr, anom2_baschr+0.1, s=5, c='r', marker="x", label='anomalous')
#ax.scatter(new_anom2_totchr, new_anom2_baschr+0.05, s=5, c='g', marker="x", label='anomalous')

#plt.legend(loc='upper left');
#plt.show()


#################################################
## Create new meternumbers, and other columns ###
#################################################

print "Working on creating new meternumbers, and other columns..."

df1['meternumber_new'] = None
SetMeters = set(df1.loc[df1.meternumber.notnull()==True].meternumber)

# New meter number scheme: simply label them 1, 2, 3, ...
NewMeterNum = {}
num = 0
for meter in SetMeters:
    num += 1
    NewMeterNum[meter] = num
df1['meternumber_new'] = [NewMeterNum.get(i) for i in df1.meternumber]

#df1['year'] = [int(str(df1.period[i])[:4]) for i in df1.index]
#df1['month'] = [int(str(df1.period[i])[4:]) for i in df1.index]

df1['period_new'] = [str(df1.period[i])[:4]+'-'+str(df1.period[i])[4:] for i in df1.index]

print "Done working on new meternumbers etc.."
print "*" *10


##############################
## Load into SQL tables ######
##############################

print "Working on loading into SQL tables..."

# First, create database in MySQL .. I called mine Meter_Water_v4

df1_Anom1_preds = df1.loc[(df1.meternumber_new.notnull()==True)&(df1.anomaly1==1)]
df1_Anom2_preds = df1.loc[(df1.meternumber_new.notnull()==True)&(df1.anomaly2==1)|(df1.anom2_newpreds==1)]
df1_Anom3_preds = df1.loc[(df1.meternumber_new.notnull()==True)&(df1.anomaly3==1)]
df1_Anom5_preds = df1.loc[(df1.meternumber_new.notnull()==True)&(df1.anomaly5==1)]

# Change to integers, otherwise SQL queries give messy results on webpage
# df1_Anom1_preds
df1_Anom1_preds['fy'] = df1_Anom1_preds['fy'].apply(lambda x: int(x))
df1_Anom1_preds['meternumber_new'] = df1_Anom1_preds['meternumber_new'].apply(lambda x: int(x))
df1_Anom1_preds['anom1_revloss'] = df1_Anom1_preds['anom1_revloss'].apply(lambda x: int(x))

# df1_Anom2_preds
df1_Anom2_preds['fy'] = df1_Anom2_preds['fy'].apply(lambda x: int(x))
df1_Anom2_preds['meternumber_new'] = df1_Anom2_preds['meternumber_new'].apply(lambda x: int(x))
df1_Anom2_preds['anom2_revloss'] = df1_Anom2_preds['anom2_revloss'].apply(lambda x: int(x))

# df1_Anom3_preds
df1_Anom3_preds['fy'] = df1_Anom3_preds['fy'].apply(lambda x: int(x))
df1_Anom3_preds['meternumber_new'] = df1_Anom3_preds['meternumber_new'].apply(lambda x: int(x))
df1_Anom3_preds['anom3_revloss'] = df1_Anom3_preds['anom3_revloss'].apply(lambda x: int(x))

# df1_Anom5_preds
df1_Anom5_preds['fy'] = df1_Anom5_preds['fy'].apply(lambda x: int(x))
df1_Anom5_preds['meternumber_new'] = df1_Anom5_preds['meternumber_new'].apply(lambda x: int(x))

# drop unnecessary columns
df1_Anom1_preds = df1_Anom1_preds.drop(['Unnamed: 0', 'service_id', 'meternumber', 'locationid','naics_code','revenue_code','period','meter_water','meter_irrigation','water','sewer','irrigation','watvolume','watcharge','sewvolume', 'sewcharge', 'irrvolume','irrcharge','residential','SFDU','multi_family','commercial','pwc','cof','wholesale','cool','industrial','inside','totvolume','totcharge','seweronlyaccount','base_charge', 'max_totvolume', 'anomaly1', 'anomaly2', 'anom2_revloss', 'peaking', 'avg_summer_use', 'median_peaking_irr_customers', 'anomaly3', 'anom3_revloss', 'spike_ratio', 'spike_95th_percentile', 'anomaly5', 'anom2_newpreds'], 1)
df1_Anom2_preds = df1_Anom2_preds.drop(['Unnamed: 0', 'service_id', 'meternumber', 'locationid','naics_code','revenue_code','period','meter_water','meter_irrigation','water','sewer','irrigation','watvolume','watcharge','sewvolume', 'sewcharge', 'irrvolume','irrcharge','residential','SFDU','multi_family','commercial','pwc','cof','wholesale','cool','industrial','inside','totvolume','totcharge','seweronlyaccount','base_charge', 'max_totvolume', 'anomaly1', 'anom1_revloss', 'anomaly2', 'peaking', 'avg_summer_use', 'median_peaking_irr_customers', 'anomaly3', 'anom3_revloss', 'spike_ratio', 'spike_95th_percentile', 'anomaly5', 'anom2_newpreds'], 1)
df1_Anom3_preds = df1_Anom3_preds.drop(['Unnamed: 0', 'service_id', 'meternumber', 'locationid','naics_code','revenue_code','period','meter_water','meter_irrigation','water','sewer','irrigation','watvolume','watcharge','sewvolume', 'sewcharge', 'irrvolume','irrcharge','residential','SFDU','multi_family','commercial','pwc','cof','wholesale','cool','industrial','inside','totvolume','totcharge','seweronlyaccount','base_charge', 'max_totvolume', 'anomaly1', 'anom1_revloss', 'anomaly2', 'anom2_revloss', 'peaking', 'avg_summer_use', 'median_peaking_irr_customers', 'anomaly3', 'spike_ratio', 'spike_95th_percentile', 'anomaly5', 'anom2_newpreds'], 1)
df1_Anom5_preds = df1_Anom5_preds.drop(['Unnamed: 0', 'service_id', 'meternumber', 'locationid','naics_code','revenue_code','period','meter_water','meter_irrigation','water','sewer','irrigation','watvolume','watcharge','sewvolume', 'sewcharge', 'irrvolume','irrcharge','residential','SFDU','multi_family','commercial','pwc','cof','wholesale','cool','industrial','inside','totvolume','totcharge','seweronlyaccount','base_charge', 'max_totvolume', 'anomaly1', 'anom1_revloss', 'anomaly2', 'anom2_revloss', 'peaking', 'avg_summer_use', 'median_peaking_irr_customers', 'anomaly3', 'anom3_revloss', 'spike_ratio', 'spike_95th_percentile', 'anomaly5', 'anom2_newpreds'], 1)

# for now, changing database to Meter_Water_v5 to test
#con = mdb.connect(user="root", password="", host="localhost", db="Meter_Water_v4", charset='utf8')
con = mdb.connect(user="root", password="", host="localhost", db="Meter_Water_v5", charset='utf8')

# The following tables are called on the webpage
df1_Anom1_preds.to_sql(con=con, name='anom1', if_exists='replace', flavor='mysql')
df1_Anom2_preds.to_sql(con=con, name='anom2', if_exists='replace', flavor='mysql')
df1_Anom3_preds.to_sql(con=con, name='anom3', if_exists='replace', flavor='mysql')
df1_Anom5_preds.to_sql(con=con, name='anom5', if_exists='replace', flavor='mysql')

print "Done loading into SQL tables!"
print "*" *10
print "All done"

# can also export to csv
# df1.to_csv('/dataset1.txt')