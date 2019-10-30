/*
 * Author: Eric McCullough
 * File Data Transformation Module
 * Trello: Goal 1
*/
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <sstream>
#include <algorithm>
#include <fstream> 

#include <ctime>

using namespace std;

/*******************************************************************************
 * Network Flow Struct
 * 
 * This struct contains the variables representing a single network flow 
 * 
 * Variables
 * ---------
 * saddr    string                           IP address of flow client
 * stime    long double                      Starting time network flow
 * ltime    long double                      Ending time of network flow
 * tbytes   long long                        Total number of bytes sent during flow
*******************************************************************************/
struct flow
{
    string saddr;
    long double stime;
    long double ltime;
    long long tbytes;
};

flow *process_csv_line(string line);
void read_csv(string fname, vector<flow *> &frame);
void processFrame(vector<flow *> &frame, int interval);
bool compareFlows(flow *f1, flow *f2);
void processFlow(string &row, vector<flow *> &frame, size_t index, const long double &min_stime, const long double &max_ltime);
void splitFlow(vector<flow *> &frame, size_t index, const long double &max_ltime);
vector<flow *>::iterator findFlow(vector<flow *> &list, const long double &key, size_t index);
void writeFrame(vector<string> &subFrame);

int main()
{
    /* TODO: modify this variable to change the time interval in seconds */
    const int interval = 60;

    // DEBUG remove later
    time_t start_read, end_read, start_iter, end_iter;
    double dur1, dur2;
    // END debug

    string fnames[] = {
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_2.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_3.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_4.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_5.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_6.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_7.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_8.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_9.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_10.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_11.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_12.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_13.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_14.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_15.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_16.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_17.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_18.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_19.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_20.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_21.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_22.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_23.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_24.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_25.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_26.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_27.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_28.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_29.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_30.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_31.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_32.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_33.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_34.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_35.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_36.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_37.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_38.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_39.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_40.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_41.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_42.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_43.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_44.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_45.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_46.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_47.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_48.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_49.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_50.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_51.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_52.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_53.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_54.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_55.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_56.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_57.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_58.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_59.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_60.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_61.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_62.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_63.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_64.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_65.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_66.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_67.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_68.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_69.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_70.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_71.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_72.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_73.csv",
        "Entire Dataset/UNSW_2018_IoT_Botnet_Dataset_74.csv"
        };
    vector<flow *> frame;

    // DEBUG remove later
    time(&start_read);
    // END debug

    for (string fname : fnames)
        read_csv(fname, frame);

    // DEBUG remove later
    cout << "SIZE BEFORE PROCESSING: " << frame.size() << endl;
    time(&end_read);
    // END debug

    // DDEBUG remove later
    time(&start_iter);
    // END debug

    processFrame(frame, interval);

    // DDEBUG remove later
    cout << "\nSIZE AFTER PROCESSING: " << frame.size() << endl;
    time(&end_iter);
    // END debug


    // DEBUG remove later
    dur1 = difftime(end_read, start_read);
    dur2 = difftime(end_iter, start_iter);
    // END debug

    cout << "Time to read: " << dur1 << endl;
    cout << "Time to iterate: " << dur2 << endl;

    return 0;
}

/*******************************************************************************
 * Read CSV Function
 * 
 * This function opens the CSVs provided in the BoT IoT Dataset and extracts the
 * information to a vector of flow structs.
 * 
 * Function Dependents
 * -------------------
 * main
 * 
 * Function Dependencies 
 * ---------------------
 * process_csv_line
*******************************************************************************/
void read_csv(string fname, vector<flow *> &frame)
{
    string line;
    ifstream csv(fname);

    if (!csv.is_open())
        throw "ERROR on opening file";

    getline(csv, line);

    while (getline(csv, line))
    {
        flow *newFlow = process_csv_line(line);
        if (newFlow != nullptr) frame.push_back(newFlow);
    }
    return;
}

/*******************************************************************************
 * Process CSV Line Function 
 * 
 * Function Dependents
 * -------------------
 * read_csv
 * main
*******************************************************************************/
flow *process_csv_line(string line)
{
    long double stime, ltime;
    string _, sstime, sltime, saddr, stbytes, proto, category, subcategory, sattack;
    long long tbytes;
    stringstream ss(line);
    bool attack;
    flow *newFlow = new flow;

    getline(ss, _, ',');           // pkSeqId
    getline(ss, sstime, ',');      // stime
    getline(ss, _, ',');           // flgs
    // getline(ss, _, ',');           // flgs_number
    getline(ss, proto, ',');       // proto
    // getline(ss, _, ',');           // proto_number
    getline(ss, saddr, ',');       // saddr
    getline(ss, _, ',');           // sport
    getline(ss, _, ',');           // daddr
    getline(ss, _, ',');           // dport
    getline(ss, _, ',');           // pkts
    getline(ss, _, ',');           // bytes
    getline(ss, _, ',');           // state
    // getline(ss, _, ',');           // state_number
    getline(ss, sltime, ',');      // ltime
    getline(ss, _, ',');           // seq
    getline(ss, _, ',');           // dur
    getline(ss, _, ',');           // mean
    getline(ss, _, ',');           // stddev
    getline(ss, _, ',');           // sum
    getline(ss, _, ',');           // min
    getline(ss, _, ',');           // max
    getline(ss, _, ',');           // spkts
    getline(ss, _, ',');           // dpkts
    getline(ss, _, ',');           // sbytes
    getline(ss, _, ',');           // dbytes
    getline(ss, _, ',');           // rate
    getline(ss, _, ',');           // srate
    getline(ss, _, ',');           // drate
    getline(ss, _, ',');           // blank cell
    getline(ss, stbytes, ',');     // TnBPSrcIP
    getline(ss, _, ',');           // TnBPDstIP
    getline(ss, _, ',');           // TnP_PsrcIP
    getline(ss, _, ',');           // TnP_PDstIP
    getline(ss, _, ',');           // TnP_PerProto
    // getline(ss, _, ',');           // TnP_Per_Dport
    // getline(ss, _, ',');           // AR_P_Proto_P_SrcIP
    getline(ss, sattack, ',');     // attack
    getline(ss, category, ',');    // category
    getline(ss, subcategory, ','); // subcateogry
    // getline(ss, _, ',');           // AR_P_Proto_P_DstIP
    // getline(ss, _, ',');           // N_IN_Conn_P_DstIP
    // getline(ss, _, ',');           // N_IN_Conn_P_SrcIP
    // getline(ss, _, ',');           // AR_P_Proto_P_Sport
    // getline(ss, _, ',');           // AR_P_Proto_P_Dport
    // getline(ss, _, ',');           // Pkts_P_State_P_Protocol_P_DestIP
    // getline(ss, _, ',');           // Pkts_P_State_P_Prrotocol_P_SrcIP
    // getline(ss, sattack, ',');     // attack
    // getline(ss, category, ',');    // category
    // getline(ss, subcategory, ','); // subcateogry

    stime = stold(sstime);
    ltime = stold(sltime);
    tbytes = stoll(stbytes);
    attack = true ? sattack == "1" : false;

    // Clean extraneous characters from strings
    saddr = saddr.substr(1, saddr.size() - 2); 
    proto = proto.substr(1, proto.size() - 2); 
    category = category.substr(1, category.size() - 2); 
    subcategory = subcategory.substr(1, subcategory.size() - 2); 
    

    /* TODO: specify appropriate filters */
    if (attack && category == "DDoS" && subcategory == "TCP") // if the traffic is a TCP DDoS
    // if (!(attack) && proto == "tcp") // if the traffic is clean tcp traffic
    {
        newFlow->saddr = saddr;
        newFlow->stime = stime;
        newFlow->ltime = ltime;
        newFlow->tbytes = tbytes;

        return newFlow;
    }
    else return nullptr;  
}

/*******************************************************************************
 * Process Frame Function
 * 
 * This function takes a vector of network flows, and processes them into input
 * for the image factory component 
 * 
 * Dependents
 * ----------
 * main
 * 
 * Dependencies 
 * ------------
 * compareFlows
 * processFlow
*******************************************************************************/
void processFrame(vector<flow *> &frame, int interval)
{
    // Sort flows by starting time
    sort(frame.begin(), frame.end(), compareFlows);

    // Loop variables
    vector<string> current_frame;
    long double min_stime = frame[0]->stime;
    long double max_ltime = min_stime + interval;

    for (size_t i = 0; i < frame.size(); ++i)
    {
        string row;
        long double current_stime = frame[i]->stime;
        long double current_ltime = frame[i]->ltime;
        // if (i % 100 == 0) cout << "\r" << i << "/" << frame.size();  
        // Flow starts in current frame and should be processed
        if (current_stime < max_ltime)
        {
            processFlow(row, frame, i, current_stime, max_ltime);
            current_frame.push_back(row);
        }
        

        // Flow starts after current frame and we should reset our loop variables
        else
        {
            // Write current frame to storage
            writeFrame(current_frame);
            // Clear out current frame
            current_frame.clear();
            min_stime = frame[i]->stime;
            max_ltime = min_stime + interval;
            processFlow(row, frame, i, current_stime, max_ltime);
            current_frame.push_back(row);
        }
        delete frame[i];
    }

    // Write leftover data to storage
    writeFrame(current_frame); 

    return;
}

/*******************************************************************************
 * Compare flow function 
 * 
 * This function is a helper function for the vector.sort method and allows it to 
 * sort a vector of pointers to flows 
 * 
 * Dependents
 * ----------
 * processFrame
*******************************************************************************/
bool compareFlows(flow *f1, flow *f2) { return (f1->stime < f2->stime); }

/*******************************************************************************
 * Process Flow Function
 * 
 * This function takes a flow and populates a row string with the appropriate 
 * information. Additionally, it will handle splitting flows if they span past 
 * the current acceptable frame. 
 * 
 * Dependents
 * ----------
 * processFrame
 * 
 * Dependencies
 * ------------
 * splitFlow 
*******************************************************************************/
void processFlow(string &row, vector<flow *> &frame, size_t index,
                 const long double &min_stime, const long double &max_ltime)
{
    if (frame[index]->stime >= min_stime && frame[index]->ltime < max_ltime)
    {
        row = frame[index]->saddr;
        row = row + ',' + to_string(frame[index]->stime) + ',' +
              to_string(frame[index]->ltime) + ',' +
              to_string(frame[index]->tbytes) + 
              '\n';
    }
    else if (frame[index]->stime >= min_stime && frame[index]->stime < max_ltime && frame[index]->ltime >= max_ltime)
    {
        splitFlow(frame, index, max_ltime);
        row = frame[index]->saddr;
        row = row + ',' + to_string(frame[index]->stime) + ',' +
              to_string(frame[index]->ltime) + ',' +
              to_string(frame[index]->tbytes) + '\n';
    }

    return;
}

/*******************************************************************************
 * Split Flow Function
 * 
 * This function takes a network flow that extends past the current time interval
 * and split it, giving a percentage of its bytes to the current window and 
 * inserting a new flow at the appropriate place in the vector 
 * 
 * Function Dependents
 * -------------------
 * processFlow
 * 
 * Function Dependencies 
 * ---------------------
 * findFlow
*******************************************************************************/
void splitFlow(vector<flow *> &frame, size_t index, const long double &max_ltime)
{
    vector<flow *>::iterator insert_loc = findFlow(frame, max_ltime, index);

    long double percent_in_frame = (max_ltime - frame[index]->stime) /
                                   (frame[index]->ltime - frame[index]->stime);

    flow *newFlow = new flow;
    newFlow->saddr = frame[index]->saddr; 
    newFlow->stime = max_ltime;
    newFlow->ltime = frame[index]->ltime; 
    newFlow->tbytes = frame[index]->tbytes * (1 - percent_in_frame);

    frame[index]->ltime = max_ltime;
    frame[index]->tbytes = frame[index]->tbytes * percent_in_frame; 

    frame.insert(insert_loc, newFlow);

    return;
}

/*******************************************************************************
 * Find Function 
 * 
 * This function finds the first instance of an object in the frame that has a
 * starting time greater than or equal to the target and returns an iterator object
 * so that splitFlow can insert the resulting flow at the correct place in the list. 
 * 
 * Function Dependents
 * -------------------
 * splitFlow
*******************************************************************************/
vector<flow *>::iterator findFlow(vector<flow *> &list, const long double &key, size_t index)
{
    for(vector<flow *>::iterator it = list.begin() + index; it < list.end(); it++) 
    {
        if ((*it)->stime >= key) return it; 
    }

    return list.end();
}

/*******************************************************************************
 * Write Frame Function
 * 
 * This function takes a subframe representing the network activity in a given 
 * time interval and writes it to an external storage system (either sql or csv)
 * 
 * Dependents
 * ----------
 * processFrame
*******************************************************************************/
void writeFrame(vector<string> &subFrame)
{
    static int num_calls = 0;
    num_calls++; 

    /* TODO: edit this line to change what directory the system writes to */
    string fname = "heat_maps/transformed_data/ddos_tcp/Frame" + to_string(num_calls) + ".csv"; 

    ofstream fout = ofstream(fname, std::ofstream::out); 
    fout << "saddr,stime,ltime,tbytes\n"; 
    for (string str : subFrame) 
    {
        fout << str; 
    }
    return;
}
