'''
Author: Eric McCullough
File: composite_frame 
Trello: Goal 1 
'''
from typing import List

import numpy as np
import pandas as pd


class Composite_Frame(object):
    '''
    The Composite_Frame class takes a pandas data frame containing network flow
    information and splits into a list of frames, each representing the telemtry
    of the network at a given time interval. 
    Dataset used: BoT IoT Dataset (10 best features CSV) 
    '''

    def __init__(self, frame: pd.DataFrame, interval: int, max_frames: int = -1):
        '''
        Instance variables:
            @self.items -> The list of dataframes derived from the parent frame
            @self._interval -> The time interval to split the parent frame on
            @self.max_frames -> The maximum number of time frames to include in
                the composite frame. Default is set to 10,000
        '''
        if max_frames < 0:
            self.max_frames = 10000
        else:
            self.max_frames = max_frames

        self.items: List[pd.DataFrame] = self._split_frame(frame, interval)
        self._interval = interval

    def _insert_row(self, row_number: int, df: pd.DataFrame, row_value):
        '''
        Borrowed this function from the following Geeks for Geeks 
        article: https://www.geeksforgeeks.org/insert-row-at-given-position-in-pandas-dataframe/
        '''
        # Split old dataframe
        df1 = df[0:row_number]
        df2 = df[row_number:]

        # Add new row to first subframe
        df1.loc[row_number] = row_value

        # Create new dataframe from two subframes
        df_result = pd.concat([df1, df2])
        df_result.index = [*range(df_result.shape[0])]

        return df_result

    def _split_flow(self, df: pd.DataFrame, index: int, interval: int, min_stime: int, max_ltime: int):
        # Insert a copy of the new row at the appropriate place in the df
        row = df.iloc[index]
        try:    # Attempt to acquire insert index
            insert_index = df[df.stime > max_ltime].index[0]
        except:  # Attempt failed, insert index should be end of dataframe
            insert_index = -1

        if insert_index >= 0:
            df = self._insert_row(int(insert_index), df, row)
        else:
            insert_index = df.shape[0]
            df.loc[insert_index] = row

        # Calculate percent of the flow which is in the current window
        percent_in_frame = (max_ltime-row.stime)/(row.ltime-row.stime)

        # Adjust values for the original flow
        df.at[index, 'ltime'] = max_ltime
        df.at[index, 'TnBPSrcIP'] = row.TnBPSrcIP*percent_in_frame

        # Adjust values for the new flowl
        df.at[insert_index, 'stime'] = max_ltime
        df.at[insert_index, 'TnBPSrcIP'] = row.TnBPSrcIP * \
            (1.-percent_in_frame)

        return df

    def _process_flow(self, current_stime, min_stime, current_ltime, max_ltime, current_frame, traffic, index, interval):
        if current_stime >= min_stime and current_ltime < max_ltime:
            current_frame.append(traffic.iloc[index])

        elif current_stime >= min_stime and current_stime < max_ltime and \
                current_ltime >= max_ltime:
            traffic = self._split_flow(
                traffic, index, interval, min_stime, max_ltime)
            current_frame.append(traffic.iloc[index])

        return traffic, current_frame

    def _split_frame(self, df: pd.DataFrame, interval: int):
        # Order the data frame by start time
        traffic = df.sort_values(by=['stime']).reset_index()
        traffic = traffic.filter(['saddr', 'stime', 'ltime', 'TnBPSrcIP'])

        # Variables for tracking the progress of the function
        progress = 0.

        # Loop Variables
        index = 0
        time_frames = []                    # list to hold subframes
        current_frame = []                  # Current data frame being populated
        # Starting point of current time frame
        min_stime = traffic.iloc[0].stime
        max_ltime = min_stime + interval    # ending point of current time frame

        # Main loop
        while not traffic.empty:
            # Find start and end time of current flow
            current_stime, current_ltime = traffic.iloc[index].stime, traffic.iloc[index].ltime

            if current_stime < max_ltime:
                traffic, current_frame = self._process_flow(current_stime, min_stime, current_ltime,
                                                            max_ltime, current_frame, traffic, index, interval)

            else:
                # Update loop variables
                current_frame = pd.DataFrame(current_frame)
                current_frame = current_frame.filter(['saddr', 'TnBPSrcIP']).groupby(
                    'saddr').sum().reset_index()
                time_frames.append(current_frame)

                # If the upper limit of frames to add has been reached, break the loop
                if len(time_frames) >= self.max_frames:
                    break

                current_frame = []
                min_stime = traffic.iloc[index].stime
                max_ltime = min_stime + interval

                traffic, current_frame = self._process_flow(current_stime, min_stime, current_ltime,
                                                            max_ltime, current_frame, traffic, index, interval)

            # If sufficient progress has been made, update the user via print
            percent_done = (index+1) / traffic.shape[0]
            if percent_done - progress >= 0.05:
                progress = percent_done
                print('Progress: {:.2f}%'.format(percent_done * 100))

            traffic = traffic.drop(index, axis=0)

            if traffic.empty:
                continue
            elif traffic.index.values[0] != 0:
                traffic.index = range(len(traffic.index))

        current_frame = pd.DataFrame(current_frame)
        time_frames.append(current_frame)
        current_frame = current_frame.filter(['saddr', 'TnBPSrcIP']).groupby(
            'saddr').sum().reset_index()

        return time_frames
