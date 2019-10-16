#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <sstream>

using namespace std;

/*
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
*/
struct flow
{
    string saddr;
    long double stime;
    long double ltime;
    long long tbytes;
};

flow *process_csv_line(string line);
void read_csv(string fname, vector<flow *> &frame);
void processFrame(vector<flow *> &frame);

int main()
{
    string fnames[] = {
        "All features/UNSW_2018_IoT_Botnet_Full5pc_1.csv",
        "All features/UNSW_2018_IoT_Botnet_Full5pc_2.csv",
        "All features/UNSW_2018_IoT_Botnet_Full5pc_3.csv",
        "All features/UNSW_2018_IoT_Botnet_Full5pc_4.csv"};
    vector<flow *> frame;
    
    for (string fname : fnames) 
        read_csv(fname, frame);

    processFrame(frame);
    return 0;
}

/*
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
*/
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
        frame.push_back(newFlow);
    }
}

/*
 * Process CSV Line Function 
 * 
 * Function Dependents 
 * -------------------
 * read_csv
 * main
*/
flow *process_csv_line(string line)
{
    long double stime, ltime;
    string _, sstime, sltime, saddr, stbytes, proto, cateogry, subcategory, sattack;
    long long tbytes;
    stringstream ss(line);
    bool attack;
    flow *newFlow = new flow;

    getline(ss, _, ',');           // pkSeqId
    getline(ss, sstime, ',');      // stime
    getline(ss, _, ',');           // flgs
    getline(ss, _, ',');           // flgs_number
    getline(ss, proto, ',');       // proto
    getline(ss, _, ',');           // proto_number
    getline(ss, saddr, ',');       // saddr
    getline(ss, _, ',');           // sport
    getline(ss, _, ',');           // daddr
    getline(ss, _, ',');           // dport
    getline(ss, _, ',');           // pkts
    getline(ss, _, ',');           // bytes
    getline(ss, _, ',');           // state
    getline(ss, _, ',');           // state_number
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
    getline(ss, stbytes, ',');     // TnBPSrcIP
    getline(ss, _, ',');           // TnBPDstIP
    getline(ss, _, ',');           // TnP_PsrcIP
    getline(ss, _, ',');           // TnP_PDstIP
    getline(ss, _, ',');           // TnP_PerProto
    getline(ss, _, ',');           // TnP_Per_Dport
    getline(ss, _, ',');           // AR_P_Proto_P_SrcIP
    getline(ss, _, ',');           // AR_P_Proto_P_DstIP
    getline(ss, _, ',');           // N_IN_Conn_P_DstIP
    getline(ss, _, ',');           // N_IN_Conn_P_SrcIP
    getline(ss, _, ',');           // AR_P_Proto_P_Sport
    getline(ss, _, ',');           // AR_P_Proto_P_Dport
    getline(ss, _, ',');           // Pkts_P_State_P_Protocol_P_DestIP
    getline(ss, _, ',');           // Pkts_P_State_P_Prrotocol_P_SrcIP
    getline(ss, sattack, ',');     // attack
    getline(ss, cateogry, ',');    // category
    getline(ss, subcategory, ','); // subcateogry

    stime = stold(sstime);
    ltime = stold(sltime);
    tbytes = stoll(stbytes);

    newFlow->saddr = saddr;
    newFlow->stime = stime;
    newFlow->ltime = ltime;
    newFlow->tbytes = tbytes;

    return newFlow;
}

/*
 * Process Frame Function
 * 
 * This function takes a vector of network flows, and processes them into input
 * for the image factory component 
 * 
 * Dependents
 * ----------
 * main
*/
void processFrame(vector<flow *> &frame)
{
    int i = 0;
    vector<string> current_frame;

    for (flow *f : frame)
    {
        string row;
        /*TODO: add code for iterating that works as the vector grows*/
        delete f;
    }
}
