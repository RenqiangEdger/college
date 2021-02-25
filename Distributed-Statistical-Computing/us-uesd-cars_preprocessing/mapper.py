#! /usr/bin/env python3

import pandas as pd 
import numpy as np
import csv
import sys

data = []
for line in csv.reader(sys.stdin):
    if len(line)!=66:
        continue
    else:
        data.append(line)

        
data_columns = ['vin', 'back_legroom', 'bed', 'bed_height', 'bed_length',
       'body_type', 'cabin', 'city', 'city_fuel_economy',
       'combine_fuel_economy', 'daysonmarket', 'dealer_zip',
       'description', 'engine_cylinders', 'engine_displacement',
       'engine_type', 'exterior_color', 'fleet', 'frame_damaged',
       'franchise_dealer', 'franchise_make', 'front_legroom',
       'fuel_tank_volume', 'fuel_type', 'has_accidents', 'height',
       'highway_fuel_economy', 'horsepower', 'interior_color', 'isCab',
       'is_certified', 'is_cpo', 'is_new', 'is_oemcpo', 'latitude',
       'length', 'listed_date', 'listing_color', 'listing_id',
       'longitude', 'main_picture_url', 'major_options', 'make_name',
       'maximum_seating', 'mileage', 'model_name', 'owner_count', 'power',
       'price', 'salvage', 'savings_amount', 'seller_rating', 'sp_id',
       'sp_name', 'theft_title', 'torque', 'transmission',
       'transmission_display', 'trimId', 'trim_name',
       'vehicle_damage_category', 'wheel_system', 'wheel_system_display',
       'wheelbase', 'width', 'year']
data = pd.DataFrame(data,columns=data_columns)
colnames_Y = ['price']
colnames_num_str = ['back_legroom', 'front_legroom', 'fuel_tank_volume', 'height', 'length', 'maximum_seating', 'transmission_display', 'wheelbase', 'width']
colnames_num_str_num = ['power', 'torque']
colnames_bool = ['franchise_dealer', 'is_new']
colnames_str = ['body_type', 'engine_cylinders', 'listing_color', 'wheel_system']
colnames_int = ['daysonmarket', 'savings_amount', 'year', 'maximum_seating', 'transmission_display']
colnames_float = ['city_fuel_economy', 'latitude', 'longitude', 'mileage', 'seller_rating', 'highway_fuel_economy', 'horsepower', 'engine_displacement', 'back_legroom', 'front_legroom', 'fuel_tank_volume', 'height', 'length', 'wheelbase', 'width', 'power', 'torque']
colnames_all = colnames_Y+colnames_bool+ colnames_str+colnames_int+colnames_float


data = data.loc[:,colnames_all]
data['price'] =  data['price'].values.astype(np.float32)
data.dropna(axis=0,subset=['price'],inplace=True)

for i in colnames_str:
    if i == 'body_type':
        temp_index = data.loc[:,i].str.contains('SUV|Crossover|Sedan').values
        temp_index1 = data.loc[:,i].str.contains('SUV|Crossover').values
        data.loc[:,i].values[temp_index1] = 'SUV'
        data.loc[:,i].values[~temp_index] = 'OthersM'
    if i == 'engine_cylinders':
        temp_index = data.loc[:,i].str.contains('I').values
        data.loc[:,i].values[temp_index] = "I"
        data.loc[:,i].values[~temp_index] = "OthersM"
    if i == 'listing_color':
        temp_index = data.loc[:,i].str.contains('BLACK|WHITE').values
        data.loc[:,i].values[~temp_index] = "OthersM"
    if i == 'wheel_system':
        temp_index = data.loc[:,i].str.contains('AWD|4WD|4X2').values
        data.loc[:,i].values[temp_index] = "AWD"
        data.loc[:,i].values[~temp_index] = "OthersM"


for i in colnames_num_str:
    data.loc[:,i] = data.loc[:,i].str.extract('\s*(\d+\.*\d*)',expand=False).astype(np.float32)
for i in colnames_num_str_num:
    temp_data =data.loc[:,i].str.extract('\s*(\d+\.*\d*).+(\d+),(\d+)',expand=True).values
    for j in range(temp_data.shape[1]):
        temp_data[:,j] = list(map(np.float32,temp_data[:,j]))
    data.loc[:,i] = temp_data[:,0]/(temp_data[:,1]*1000+temp_data[:,2])
    
for i in colnames_int[:-2] + colnames_float[:-9]:
    data.loc[:,i] = data.loc[:,i].str.extract('\s*(-*\d+\.*\d*)',expand=False).astype(np.float32)


for i in colnames_bool:
    data.loc[:,i] = np.where(data.loc[:,i]=='True',1,0)
    
dummys_prefix = {'body_type':'body_type','engine_cylinders':'engine','listing_color':'color','wheel_system':'WS'}
drop_dummys_col = [i+'_OthersM' for i in dummys_prefix.values()]
data = pd.get_dummies(data,prefix=dummys_prefix,columns=colnames_str)
data.drop(columns=drop_dummys_col,inplace=True)

fillna_dict = { 'daysonmarket': 76.92, 'savings_amount': 813.37, 'year': 2017, 'maximum_seating': 5,
 'transmission_display': 6, 'city_fuel_economy': 22.12, 'latitude': 41.13, 'longitude': -75.45, 'mileage': 33897.54,
 'seller_rating': 4.06, 'highway_fuel_economy': 29.10, 'horsepower': 246.31, 'engine_displacement': 2802.96,
 'back_legroom': 37.58, 'front_legroom': 42.11, 'fuel_tank_volume': 18.25, 'height': 64.69,
 'length': 190.39, 'wheelbase': 112.92, 'width': 78.57, 'power': 0.0439, 'torque': 0.0945}

data.fillna(value=data.mean(),inplace=True)
data.loc[:,'year'] = 2020-data.loc[:,'year']

n = data.shape[0]

x = np.hstack((np.repeat(1,n).reshape(n,1),data.values[:,1:]))
xt = x.T
xtx = np.dot(xt,x)
xty = np.dot(xt,data.values[:,0])
xty_xtx = np.append(xty,xtx.flatten('F'))
s_format1 = '\t'.join(['%f']*len(xty_xtx))
print(s_format1%tuple(xty_xtx))


