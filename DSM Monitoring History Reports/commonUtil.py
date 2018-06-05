# import required lib
import datetime
import re
import time
import numpy as np
import pylab as pl

from scipy.interpolate import spline

def is_valid_datetime(end_time):
    try:
        time.strptime(end_time, '%Y-%m-%d %H:%M:%S')
        return True
    except:
        return False

'''
In corresponding notebook single-db-resource-consumption, 
we pass value of DB_CONN_ID, END_TIME, INTERVAL, REPORT_TYPE to "singleDBCondition" as a String
This function is to split "singleDBCondition" and assign the values to variables 
'''
def handle_query_condition():
    try:
        END_TIME = ''
        INTERVAL = 0
        START_TIME = ''
        REPORT_TYPE = ''
        ISOK_ALL_PARA = 1
        GENERAL_ERROR = []
        DB_SET = []
        TBSP_NAME = ''
        queryCon = {}

        queryCondition = None
        with open("queryCondition.txt") as filetmp:
            queryCondition = filetmp.read()

        queryCondition = re.match(r'DB_CONN_ID\s?=\s?.+END_TIME\s?=\s?.+INTERVAL\s?=\s?.+REPORT_TYPE\s?=\s?.+', queryCondition)

        if (queryCondition != None):
            all_vars = queryCondition.group().split(' ')
            if (len(all_vars) != 4) and (len(all_vars) != 5):
                err_str = 'The format of parameters is error.'
                if err_str not in GENERAL_ERROR:
                    GENERAL_ERROR.append(err_str)
                ISOK_ALL_PARA = 0
            else:  # len(all_vars) == 4 or 5
                if len(all_vars) == 4:
                    # get value of REPORT_TYPE
                    if (re.search(r'REPORT_TYPE\s?=\s?(.+)', all_vars[3]) == None):
                        err_str = 'The format of REPORT_TYPE error.'
                        if err_str not in GENERAL_ERROR:
                            GENERAL_ERROR.append(err_str)
                        ISOK_ALL_PARA = 0
                    else:
                        REPORT_TYPE = re.search(r'REPORT_TYPE\s?=\s?(.+)', all_vars[3]).group(1).upper().replace("'", "")
                        if (REPORT_TYPE != 'ALL' and REPORT_TYPE != 'RESOURCE' and REPORT_TYPE != 'CPU' \
                                and REPORT_TYPE != 'MEMORY' and REPORT_TYPE != 'LOG' and REPORT_TYPE != 'TBSP_TABLE' and REPORT_TYPE != 'DATABASE'):
                            err_str = 'The format of REPORT_TYPE error.'
                            if err_str not in GENERAL_ERROR:
                                GENERAL_ERROR.append(err_str)
                            ISOK_ALL_PARA = 0

                    # get value of DB_CONN_ID
                    DB_CONN_ID = re.search(r'DB_CONN_ID\s?=\s?(.+)', all_vars[0])
                    if DB_CONN_ID == None:
                        err_str = 'The DB_CONN_ID can not be empty.'
                        if err_str not in GENERAL_ERROR:
                            GENERAL_ERROR.append(err_str)
                        ISOK_ALL_PARA = 0
                    else:
                        DB_CONN_ID = DB_CONN_ID.group(1).replace("'", "")
                        if (REPORT_TYPE == 'DATABASE'):
                            if DB_CONN_ID != '*':
                                err_str = 'The DB_CONN_ID must be *.'
                                if err_str not in GENERAL_ERROR:
                                    GENERAL_ERROR.append(err_str)
                                ISOK_ALL_PARA = 0
                        else:
                            DB_SET = DB_CONN_ID.split(',')

                    # get value of INTERVAL
                    if (re.search(r'INTERVAL\s?=\s?([0-9]+$)', all_vars[2]) == None):
                        err_str = 'The format of INTERVAL error.'
                        if err_str not in GENERAL_ERROR:
                            GENERAL_ERROR.append(err_str)
                        ISOK_ALL_PARA = 0
                    else:
                        INTERVAL = int(re.search(r'(\d+)', all_vars[2]).group())
                        if (INTERVAL > 100):  # most get 100 data
                            INTERVAL = 100

                    # get START_TIME bases on END_TIME and INTERVAL
                    if (re.search(r'(\d{4}-\d{1,2}-\d{1,2})', all_vars[1]) == None) | (
                            re.search(r'(\d{1,2}:\d{1,2}:\d{1,2})', all_vars[1]) == None):
                        err_str = 'The format of END_TIME error.'
                        if err_str not in GENERAL_ERROR:
                            GENERAL_ERROR.append(err_str)
                        ISOK_ALL_PARA = 0
                    else:
                        END_TIME = re.search(r'(\d{4}-\d{1,2}-\d{1,2})', all_vars[1]).group() + ' ' + re.search(r'(\d{1,2}:\d{1,2}:\d{1,2})', all_vars[1]).group()
                        if is_valid_datetime(END_TIME) == True:
                            datetime_tuple = time.strptime(END_TIME, '%Y-%m-%d %H:%M:%S')
                            # To slice datetime_tuple to gain exact time data
                            year, month, day, hour, minute, second = datetime_tuple[:6]
                            final_time = datetime.datetime(year, month, day, hour, minute, second) + datetime.timedelta(
                                hours=-INTERVAL)
                            # convert datetime fields to string
                            START_TIME = final_time.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            err_str = 'The format of END_TIME error.'
                            if err_str not in GENERAL_ERROR:
                                GENERAL_ERROR.append(err_str)
                            ISOK_ALL_PARA = 0

                if len(all_vars) == 5:
                    # To get the value of TBSP_NAME
                    if (re.search(r'TBSP_NAME\s?=\s?(.+)', all_vars[1]) == None):
                        err_str = 'The format of TBSP_NAME error.'
                        if err_str not in GENERAL_ERROR:
                            GENERAL_ERROR.append(err_str)
                        ISOK_ALL_PARA = 0
                    else:
                        TBSP_NAME = re.search(r'TBSP_NAME\s?=\s?(.+)', all_vars[1]).group(1).upper()

                    # To get the value of REPORT_TYPE
                    if (re.search(r'REPORT_TYPE\s?=\s?(.+)', all_vars[4]) == None):
                        err_str = 'The format of REPORT_TYPE error.'
                        if err_str not in GENERAL_ERROR:
                            GENERAL_ERROR.append(err_str)
                        ISOK_ALL_PARA = 0
                    else:
                        REPORT_TYPE = re.search(r'REPORT_TYPE\s?=\s?(.+)', all_vars[4]).group(1).upper()
                        if (REPORT_TYPE != 'ALL' and REPORT_TYPE != 'TABLESPACE' and REPORT_TYPE != 'DATABASE' and REPORT_TYPE != 'TBSP_TABLE'):
                            err_str = 'The format of REPORT_TYPE error.'
                            if err_str not in GENERAL_ERROR:
                                GENERAL_ERROR.append(err_str)
                            ISOK_ALL_PARA = 0

                    # To get DB_CONN_ID
                    if (re.search(r'DB_CONN_ID\s?=\s?(.+)', all_vars[0]) == None):
                        err_str = 'The DB_CONN_ID can not be empty.'
                        if err_str not in GENERAL_ERROR:
                            GENERAL_ERROR.append(err_str)
                        ISOK_ALL_PARA = 0
                    else:
                        DB_CONN_ID = re.search(r'DB_CONN_ID\s?=\s?(.+)', all_vars[0]).group(1)
                        if(REPORT_TYPE == 'DATABASE'):
                            if DB_CONN_ID != '*':
                                err_str = 'The DB_CONN_ID must be *.'
                                if err_str not in GENERAL_ERROR:
                                    GENERAL_ERROR.append(err_str)
                                ISOK_ALL_PARA = 0
                        else:
                            DB_SET = DB_CONN_ID.split(',')
                    # To get the value of INTERVAL
                    if (re.search(r'INTERVAL\s?=\s?([0-9]+$)', all_vars[3]) == None):
                        err_str = 'The format of INTERVAL error.'
                        if err_str not in GENERAL_ERROR:
                            GENERAL_ERROR.append(err_str)
                        ISOK_ALL_PARA = 0
                    else:
                        INTERVAL = int(re.search(r'(\d+)', all_vars[3]).group())
                        if (INTERVAL > 100):  # most get 100 data
                            INTERVAL = 100

                    # To get START_TIME according to END_TIME
                    if (re.search(r'(\d{4}-\d{1,2}-\d{1,2})', all_vars[2]) == None) | (re.search(r'(\d{1,2}:\d{1,2}:\d{1,2})', all_vars[2]) == None):
                        err_str = 'The format of END_TIME error.'
                        if err_str not in GENERAL_ERROR:
                            GENERAL_ERROR.append(err_str)
                        ISOK_ALL_PARA = 0
                    else:
                        END_TIME = re.search(r'(\d{4}-\d{1,2}-\d{1,2})', all_vars[2]).group() + ' ' + re.search( r'(\d{1,2}:\d{1,2}:\d{1,2})', all_vars[2]).group()
                        if is_valid_datetime(END_TIME) == True:
                            datetime_tuple = time.strptime(END_TIME, '%Y-%m-%d %H:%M:%S')
                            # To slice datetime_tuple to gain exact time data
                            year, month, day, hour, minute, second = datetime_tuple[:6]
                            final_time = datetime.datetime(year, month, day, hour, minute, second) + datetime.timedelta(hours = -INTERVAL)
                            # To transfer the datetime fields to string
                            START_TIME = final_time.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            err_str = 'The format of END_TIME error.'
                            if err_str not in GENERAL_ERROR:
                                GENERAL_ERROR.append(err_str)
                            ISOK_ALL_PARA = 0
        else:
            err_str = 'The format of parameters is error.'
            if err_str not in GENERAL_ERROR:
                GENERAL_ERROR.append(err_str)
            ISOK_ALL_PARA = 0
        queryCon["DB_SET"] = DB_SET
        queryCon["END_TIME"] = END_TIME
        queryCon["INTERVAL"] = INTERVAL
        queryCon["START_TIME"] = START_TIME
        queryCon["REPORT_TYPE"] = REPORT_TYPE
        queryCon["ISOK_ALL_PARA"] = ISOK_ALL_PARA
        queryCon["GENERAL_ERROR"] = GENERAL_ERROR
        queryCon["TBSP_NAME"] = TBSP_NAME
        return queryCon
    except Exception as e:
        return str(e)

'''
This function is to get all datatime string according to the result 
queried from database.
The variable ori_datetime_str is a dictionary and its data 
from the combination of hour_list and date_all, which will 
be used for judging whether some data exists in it or not.
'''


def get_original_datatime_str(date_all, hour_list):
    try:
        ori_datetime_str = {}
        for indx in range(len(hour_list)):
            tmp_date_str = date_all[indx].encode('unicode-escape').decode('string_escape')
            tmp_hour_str = str(hour_list[indx])
            tmp_datetime_str = ''
            if (len(tmp_hour_str) == 1):  # Change 1:00:00 into 01:00:00
                tmp_hour_str = '0' + tmp_hour_str
            tmp_datetime_str = tmp_date_str + ' ' + tmp_hour_str + ':00:00'
            ori_datetime_str[tmp_datetime_str] = indx
        return ori_datetime_str
    except Exception as e:
        return str(e)


# To store previous date string value as a reference data
def format_x_axis(date_all, hour_list, x_ticks, x_ticks_lables):
    try:
        pre_date_str = str(date_all[0])
        for dateIdx in range(len(date_all)):
            # To get the data for the x-axis ticks
            x_ticks.append(float('%0.1f' % dateIdx))
            '''
            Get the data for the lable of x-axis
            If the label existed in the list x_ticks_lables,
            put hour_str into x_ticks_lables 
            otherwise,put date_lables into x_ticks_lables
            '''
            hour_str = str(hour_list[dateIdx])
            if len(hour_str) == 1:
                hour_str = '0' + hour_str
            date_str = str(date_all[dateIdx])
            x_lables = date_str + ' ' + hour_str
            if (dateIdx == 0):
                x_ticks_lables.append(x_lables)
            else:  # dateIdx > 0
                if (pre_date_str == date_str):
                    x_ticks_lables.append(hour_str)
                else:
                    pre_date_str = date_str
                    x_ticks_lables.append(x_lables)
    except Exception as e:
        return str(e)


'''
This function is to fill data when there ia no data on this hour in repository
database.
1): complete x ticks via all hours, if no this hour data, add it.
2): if no data is in some hour, the value 0.0 will be filled.
3): form x ticks labels according to specific hour
@return date array for y axis
'''


def fill_null_data(START_TIME, END_TIME, date_all, x_hour_list, y_ori_data, ori_datetime_str):
    try:
        # To transfer datatime data into time tuple for getting its' timestamp
        tm_tuple = time.strptime(START_TIME, '%Y-%m-%d %H:%M:%S')
        min_timestamp = time.mktime(tm_tuple)

        tmp_tuple = time.strptime(END_TIME, '%Y-%m-%d %H:%M:%S')
        max_timestamp = time.mktime(tmp_tuple)

        y_new_data_list = []

        '''
        To get the difference between the max_timestamp and min_timestamp 
        which will be used for gaining all date and hour including the missing
        '''
        hour_diff = int((max_timestamp - min_timestamp) / 3600)

        # Reassign above vaiables:date_all / x_hour_list
        for tmp_id in range(hour_diff):
            tmp_st = min_timestamp + tmp_id * 3600
            # To change timestamp to datetime string
            tmp_datetime = datetime.datetime.fromtimestamp(tmp_st)
            tmp_datetime_str = tmp_datetime.strftime("%Y-%m-%d %H:00:00")
            tmp_date_str = tmp_datetime_str[0:10]
            tmp_hour_str = tmp_datetime_str[11:13]
            date_all.append(tmp_date_str)
            x_hour_list.append(tmp_hour_str)

            '''
            Below code is for handling y-axis's data
            If there is no data on this hour,0.0 will be filled
            '''
            if tmp_datetime_str in ori_datetime_str:
                tmp_index = ori_datetime_str[tmp_datetime_str]
                y_new_data_list.append(list(y_ori_data)[tmp_index])
            else:
                y_new_data_list.append(0.0)

        return np.asarray(y_new_data_list)
    except Exception as e:
        return str(e)


'''
Create a figure object for graph and set 
1): figure size according to data quantity
2): figure title/x label/y label/grid/xticks
@return figure object
'''


def declare_figure(x_ticks, x_ticks_lables, figure_title, x_label, y_label):
    try:
        # To declare a Sketchpad including two graphs of left and right distribution
        fig = pl.figure()
        # To set the size of the Sketchpad
        data_size = len(x_ticks)
        if (data_size <= 20):
            fig.set_size_inches(12, 6)
        elif (data_size <= 40):
            fig.set_size_inches(16, 6)
        elif (data_size <= 60):
            fig.set_size_inches(18, 7)
        elif (data_size <= 100):
            fig.set_size_inches(22, 7)
        # To set the title for the left graph
        pl.title(figure_title, fontsize=14, fontweight='bold')
        pl.xticks(x_ticks, x_ticks_lables, rotation=90)
        # To set x-axis label
        pl.xlabel(x_label)
        # To set y-axis label
        pl.ylabel(y_label)
        # To set grid format for ax1
        pl.grid(True, ls='-.', color='#a6266e', linewidth='0.2', alpha=0.3)
        return fig
    except Exception as e:
        return str(e)


'''
Draw graph according to the data x axis and y axis
1): smooth the line
2): mark the data for points every other
'''


def draw_point_line(x_ticks, y_data):
    try:
        # To mark the data point
        y_data_list = list(y_data)
        for id in range(0, len(y_data_list), 2):
            if (y_data_list[id] == 0.0):  # If no data,drawing a empty circle
                pl.scatter(x_ticks[id], y_data_list[id], c='', marker='o', edgecolors='r', s=50)
            else:
                pl.scatter(x_ticks[id], y_data_list[id], c='#2c628b')
                pl.text(x_ticks[id], y_data_list[id], '%.2f' % y_data_list[id], fontsize=9)

        '''
        if the size of data is greater than 2, showing a line not a scatter
        otherwise, a scatter graph will be showed
        '''
        if (len(x_ticks) >= 3):
            # Below two variable is used for storeing magnified data
            xnew_hour = []
            ynew_data = []
            ##In order to smooth the line chart,handle the data further##
            # Expand each x axis data 20 times
            xnew_hour = np.linspace(np.asarray(x_ticks).min(), np.asarray(x_ticks).max(), np.asarray(x_ticks).size * 20)
            # To handle the data of new y axis data
            ynew_data = spline(np.asarray(x_ticks), y_data, xnew_hour)
            ynew_data_list = list(ynew_data)
            # If the value of ynew_cpu_sec_list is negative, then change it into positive
            for y_idx in range(len(ynew_data_list)):
                if (ynew_data_list[y_idx] < 0.0):
                    ynew_data_list[y_idx] = 0.0
            ynew_data = np.asarray(ynew_data_list)
            # To fill the gragh according to your requirement
            pl.fill_between(xnew_hour, ynew_data, where=(xnew_hour.min() < xnew_hour) & (xnew_hour < xnew_hour.max()),
                            color='#2c628b', alpha=0.09)
            # To draw curve graph
            pl.plot(xnew_hour, ynew_data, color='#2c628b')
            # To set y-axis value range according to your data
            start_value = ynew_data.min() - ynew_data.min() / 2
            start_value = float('%.1f' % start_value)
            end_value = ynew_data.max() + ynew_data.max() / 2
            end_value = float('%.1f' % end_value)
            # To set the scale for y-axis
            pl.ylim(float('%.1f' % start_value), float('%.2f' % end_value))
            # pl.yticks(np.linspace(start_value,end_value,0.1,endpoint=True))
        pl.show()
    except Exception as e:
        return str(e)

def get_all_times(list_all, list_distinct):
    try:
        distinct_times = {}
        for idx in range(len(list_distinct)):
            cnt = 0
            for idx_1 in range(len(list_all)):
                if list_distinct[idx] == list_all[idx_1]:
                    cnt += 1
            distinct_times[str(list_distinct[idx])] = cnt
        return distinct_times
    except Exception as e:
        print
        str(e)


def getmaxtime(conn_times):
    try:
        maxtime = 1
        all_times = conn_times.values()
        for idx in range(len(all_times)):
            if maxtime < all_times[idx]:
                maxtime = all_times[idx]
        return maxtime
    except Exception as e:
        print
        str(e)


'''
Compute the mean rank according to mean cpu usage
'''


def get_sort_circle(mean_cpu_usage):
    try:
        circle_size = {}
        dbname_keys = []
        cpu_values = []
        tmp_dict = {}
        ##Get no duplicates values
        for k, v in mean_cpu_usage.items():
            dbname_keys.append(k)
            cpu_values.append(v)
        cpu_values = sorted(list(set(cpu_values)))

        ##Form a new dictionary to store above key and rank value
        for i in range(len(cpu_values)):
            tmp_dict[cpu_values[i]] = i + 1

        # Generate a new dictionary using new rank
        for key in mean_cpu_usage:
            for key1 in tmp_dict:
                if mean_cpu_usage[key] == key1:
                    circle_size[key] = tmp_dict[key1]
        return circle_size

    except Exception as e:
        print
        str(e)


def get_mean_rank(all_conn_names, distinct_conn_names, all_ranks):
    try:
        y_data = []
        y_data_dict = {}
        '''
        using the value from the score list to compute each database's average rank
        different database may have data in some week,if its rank is 1, it can get 13 scores
        do every database like this.
        '''
        score_list = {1: 13, 2: 8, 3: 5, 4: 3, 5: 2}
        for i in range(len(distinct_conn_names)):
            tmp_cnt = 0
            tmp_score = 0
            for j in range(len(all_conn_names)):
                if str(distinct_conn_names[i]) == str(all_conn_names[j]):
                    tmp_cnt += 1
                    tmp_score += score_list[all_ranks[j]]
            float_rank = float('%.1f' % (tmp_score * 1.0 / tmp_cnt))
            y_data.append(float_rank)
            y_data_dict[str(distinct_conn_names[i])] = float_rank
        # remove duplicate value and reverse the temporary list
        float_sorted_rank = list(reversed(sorted(list(set(y_data)))))
        int_rank_dict = {}
        ##Form a new dictionary to store above key and rank value
        for k in range(len(float_sorted_rank)):
            int_rank_dict[float_sorted_rank[k]] = k + 1

        ## y_data used to draw y-axis
        for f in range(len(y_data)):
            for key in int_rank_dict:
                if (y_data[f]) == key:
                    y_data[f] = 6 - int_rank_dict[key]
                    break
        return y_data

    except Exception as e:
        print
        str(e)