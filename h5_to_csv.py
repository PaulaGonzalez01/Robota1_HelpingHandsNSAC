import h5py
import os
import csv
from threading import Thread
from points import points_h5
import random


def get_month(file=''):
    date = list(file.split('_')[4])
    m1 = date[4]
    m2 = date[5]
    return m1 + m2


def get_day(file=''):
    date = list(file.split('_')[4])
    m1 = date[6]
    m2 = date[7]
    return m1 + m2


added_latlng = {}


def exists(latLong, csv_des):
    if latLong in added_latlng:
        return added_latlng[latLong]
    return -1


csv_dest = []
csv_columns = ['latitude', 'longitude']
for i in range(1, 13):
    csv_columns.append('soil_moisture_'+str(i).zfill((2)))
    csv_columns.append('surface_temperature_'+str(i).zfill((2)))
files = []
lens = [0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

f_dest = 'S_DEST'
upper = [11.696331, -86.003182]
lower = [-55.715245, -19.998368]

# Compute indexes

indexes = []
h5f = h5py.File('smapl3/2016/01/SMAP_L3_SM_P_20160101_R16510_001.h5', 'r')
ds_lat = h5f['Soil_Moisture_Retrieval_Data_AM']['latitude']
ds_lng = h5f['Soil_Moisture_Retrieval_Data_AM']['longitude']
# print("Computing indexes")
# for i in range(0, 406):
#     for j in range(0, 964):
#         lat = ds_lat[i][j]
#         lng = ds_lng[i][j]
#         if str(lat) + ',' + str(lng) in points_h5:
#             indexes.append((i, j))
# print("Computed indexes: " + str(len(indexes)))

# targets = (
#     [{'f': 'SMAP_L3_SM_P_20160101_R16510_001.h5', 'm': '01', 'd': '01'}], [],)
targets = ([], [],)

# r = root, d = directories, f = files
for r, d, f in os.walk('smapl3/2017/01'):
    for file in f:
        if file.endswith('.h5'):
            m = get_month(file)
            d = get_day(file)
            # if int(m) % 2 == 0:
            # targets[0].append({'f': file, 'm': m, 'd': d})
            targets[0].append({'f': file, 'm': m, 'd': d})
            # else:
            #     targets[1].append({'f': file, 'm': m, 'd': d})


def crawl(files=[], csv_dest=[], t=''):
    for data in files:
        file = data['f']
        m = data['m']
        d = data['d']
        h5f = h5py.File('smapl3/2017/' + m + '/' + file, 'r')
        ds_sm = h5f['Soil_Moisture_Retrieval_Data_AM']['soil_moisture']
        ds_st = h5f['Soil_Moisture_Retrieval_Data_AM']['surface_temperature']
        ds_lat = h5f['Soil_Moisture_Retrieval_Data_AM']['latitude']
        ds_lng = h5f['Soil_Moisture_Retrieval_Data_AM']['longitude']
        indexes_indexes = list(range(len(indexes)))
        random.shuffle(indexes_indexes)
        soil_key = 'soil_moisture_' + m
        tmp_key = 'surface_temperature_' + m

        for i in range(0, 406):
            for j in range(0, 964):
                lat = ds_lat[i][j]
                lng = ds_lng[i][j]
                if lat < upper[0] and lat > lower[0] and lng > upper[1] and lng < lower[1]:
                    sm = ds_sm[i][j]
                    st = ds_st[i][j]
                    lat_lng_str = str(lat)+","+str(lng)
                    lat_str = str(lat)
                    lng_str = str(lng)
                    index = exists(lat_lng_str, csv_dest)
                    if index != -1:
                        # print(t + ": Exists " + lat_lng_str + " m:" + m)
                        if (soil_key) in csv_dest[index]:
                            csv_dest[index][soil_key] = (
                                csv_dest[index][soil_key][0] + sm, csv_dest[index][soil_key][1] + 1)
                            csv_dest[index][tmp_key] = (
                                csv_dest[index][tmp_key][0] + st, csv_dest[index][tmp_key][1] + 1)
                        else:
                            csv_dest[index][soil_key] = (sm, 1)
                            csv_dest[index][tmp_key] = (st, 1)
                    else:
                        print(t + ": Doesnt exists " +
                              lat_lng_str + " m:" + m + " d: " + d)
                        csv_dest.append({
                            'latitude': lat_str,
                            'longitude': lng_str,
                            soil_key: (sm, 1),
                            tmp_key: (st, 1),
                        })
                        added_latlng[lat_lng_str] = len(csv_dest) - 1
                    iindex = exists(lat_lng_str, csv_dest)
                    count = csv_dest[iindex][soil_key][1]
                    if csv_dest[iindex][soil_key][0] == -9999.0:
                        csv_dest[iindex][soil_key] = (0, count - 1)
                    if csv_dest[iindex][tmp_key][0] == -9999.0:
                        csv_dest[iindex][tmp_key] = (
                            0, csv_dest[iindex][tmp_key][1] - 1)
                    else:
                        csv_dest[iindex][tmp_key] = (
                            csv_dest[iindex][tmp_key][0] - 273.15, csv_dest[iindex][tmp_key][1])

                    if lens[int(m)] == count:
                        print(t + ": Done with " + m)
                        csv_dest[iindex][soil_key] = (
                            csv_dest[iindex][soil_key] / lens[int(m)], 99)
                        csv_dest[iindex][tmp_key] = (
                            csv_dest[iindex][tmp_key] / lens[int(m)], 99)
        print(t + " done day: " + d)
        h5f.close()
        with open('names422017.csv', 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            writer.writerows(csv_dest)


# threads = []
# for ii in range(len(targets)):
#     # We start one thread per url present.
#     data = targets[ii]
#     process = Thread(target=crawl, args=[data, csv_dest, str(ii)])
#     process.start()
#     threads.append(process)

# for process in threads:
#     process.join()

crawl(targets[0], csv_dest, '1')
currentPath = os.getcwd()
csv_file = currentPath + "/csv/Names23.csv"

with open('names422017.csv', 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
    writer.writeheader()
    writer.writerows(csv_dest)
