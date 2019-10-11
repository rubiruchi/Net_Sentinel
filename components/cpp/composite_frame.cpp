#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <sstream>

using namespace std;

struct flow
{
    string saddr;
    long double stime;
    long double ltime;
    long long tbytes;
};

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

vector<flow *> read_csv(std::string fname)
{
    string line;
    ifstream csv(fname);
    vector<flow *> dataframe; 

    // DELETE LATER
    int i = 0;

    if (!csv.is_open())
        throw "ERROR on opening file";

    getline(csv, line); 

    while (getline(csv, line))
    {
        flow *newFlow = process_csv_line(line);
        dataframe.push_back(newFlow); 

        // DELETE LATER
        i++; 
        if (i >= 5)
            break; 
    }

    return dataframe; 
}

int main()
{
    string fname = "All features/UNSW_2018_IoT_Botnet_Full5pc_1.csv"; 
    vector<flow *> frame = read_csv(fname); 

    int vec_len = frame.size();
    for (int i=0; i < vec_len; ++i)
    {
        flow *aFlow = frame.front();
        frame.erase(frame.begin());  

        cout << "My Flow: " << endl;
        cout << "\t" << aFlow->saddr << endl;
        cout << "\t" << aFlow->stime << endl;
        cout << "\t" << aFlow->ltime << endl;
        cout << "\t" << aFlow->tbytes << endl;

        delete aFlow;
    }

    return 0; 
}
