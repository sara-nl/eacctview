import csv
import re
from subprocess import Popen, PIPE
import os

import numpy as np
import plotext as plx


class Plotter():
    def __init__(self):

        self.avgdata = {}
        self.loopdata = {}

        self.job_id = None
        self.step_id = None

        self.loops_status=False
        self.loop_metrics = ["AVG_CPUFREQ_KHZ","AVG_IMCFREQ_KHZ","DEF_FREQ_KHZ","CPI","TPI","MEM_GBS","IO_MBS","PERC_MPI","DC_NODE_POWER_W","DRAM_POWER_W","PCK_POWER_W","GFLOPS","L1_MISSES","L2_MISSES","L3_MISSES","SPOPS_SINGLE","SPOPS_128","SPOPS_256","SPOPS_512","DPOPS_SINGLE","DPOPS_128","DPOPS_256","DPOPS_512","TEMP0","TEMP1","TEMP2","TEMP3","GPU0_POWER_W","GPU0_FREQ_KHZ","GPU0_MEM_FREQ_KHZ","GPU0_UTIL_PERC","GPU0_MEM_UTIL_PERC","GPU0_GFLOPS","GPU0_TEMP","GPU0_MEMTEMP","GPU1_POWER_W","GPU1_FREQ_KHZ","GPU1_MEM_FREQ_KHZ","GPU1_UTIL_PERC","GPU1_MEM_UTIL_PERC","GPU1_GFLOPS","GPU1_TEMP","GPU1_MEMTEMP","GPU2_POWER_W","GPU2_FREQ_KHZ","GPU2_MEM_FREQ_KHZ","GPU2_UTIL_PERC","GPU2_MEM_UTIL_PERC","GPU2_GFLOPS","GPU2_TEMP","GPU2_MEMTEMP","GPU3_POWER_W","GPU3_FREQ_KHZ","GPU3_MEM_FREQ_KHZ","GPU3_UTIL_PERC","GPU3_MEM_UTIL_PERC","GPU3_GFLOPS","GPU3_TEMP","GPU3_MEMTEMP","GPU4_POWER_W","GPU4_FREQ_KHZ","GPU4_MEM_FREQ_KHZ","GPU4_UTIL_PERC","GPU4_MEM_UTIL_PERC","GPU4_GFLOPS","GPU4_TEMP","GPU4_MEMTEMP","GPU5_POWER_W","GPU5_FREQ_KHZ","GPU5_MEM_FREQ_KHZ","GPU5_UTIL_PERC","GPU5_MEM_UTIL_PERC","GPU5_GFLOPS","GPU5_TEMP","GPU5_MEMTEMP","GPU6_POWER_W","GPU6_FREQ_KHZ","GPU6_MEM_FREQ_KHZ","GPU6_UTIL_PERC","GPU6_MEM_UTIL_PERC","GPU6_GFLOPS","GPU6_TEMP","GPU6_MEMTEMP","GPU7_POWER_W","GPU7_FREQ_KHZ","GPU7_MEM_FREQ_KHZ","GPU7_UTIL_PERC","GPU7_MEM_UTIL_PERC","GPU7_GFLOPS","GPU7_TEMP","GPU7_MEMTEMP","LOOP_SIZE"]

        self.filename = "tmp.csv"
        self.arch_spec_file = os.path.dirname(os.path.abspath(__file__)) + "/data/architecture_specs.csv"

        #Roofline Metrics
        self.arch_name = None
        self.arch_ncores = None
        self.arch_freq = None
        self.arch_NDPs = None
        self.arch_DP_RPEAK = None
        self.arch_SP_RPEAK = None
        self.arch_HP_RPEAK = None
        self.arch_DRAMBW = None 

        # Color Palettes
        self.arch_palette ={"Rome": "tab:blue",
                            "Genoa": "tab:orange",
                            "A100": "tab:green",
                            "H100": "tab:red",
                            "Fat_Rome": "tab:purple",
                            "Fat_Genoa": "tab:brown"
                            }


    def get_jobid(self,jobid_from_args):

        if len(jobid_from_args[0].split(".")) == 1:
            self.job_id = jobid_from_args[0]
            self.step_id = "0"
        elif len(jobid_from_args[0].split(".")) == 2:
            self.job_id = jobid_from_args[0].split(".")[0]
            self.step_id = jobid_from_args[0].split(".")[1]
        print("Querying: (jobid,stepid) ("+self.job_id+","+self.step_id+")")


    def get_architecture_specs(self, data):

        with open(self.arch_spec_file) as csvfile:
            reader = csv.DictReader(csvfile, delimiter=";")
            for row in reader:
                if row["NAME"] == data['Arch']:

                    self.arch_name = row['NAME']
                    self.arch_ncores = float(row['NCORES'])
                    self.arch_freq = float(row['CPU_FREQ_GHZ'])
                    self.arch_NDPs = float(row['NDPS'])
                    self.arch_memory_freq = float(row['MEM_FREQ_MHZ'])
                    self.arch_memory_channels = float(row['N_MEM_CHANNELS'])
                    self.arch_DP_RPEAK = self.arch_ncores * self.arch_freq * self.arch_NDPs
                    self.arch_SP_RPEAK = self.arch_DP_RPEAK * 2.0
                    self.arch_HP_RPEAK = self.arch_DP_RPEAK * 4.0
                    # DRAMBW = (bits/bytes) * Mem_freq * N channels * (Mega/Giga)
                    self.arch_DRAMBW = (64./8.) * self.arch_memory_freq * self.arch_memory_channels * (1e6/1e9) 

        return(data)

    def set_architecture_specs(self, data):

        self.arch_name = data['Arch']
        self.arch_ncores = data['NCORES']
        self.arch_freq = data['CPU_FREQ_GHZ']
        self.arch_NDPs = data['NDPS']
        self.arch_DP_RPEAK = self.arch_ncores * self.arch_freq * self.arch_NDPs
        self.arch_SP_RPEAK = self.arch_DP_RPEAK * 2.0
        self.arch_HP_RPEAK = self.arch_DP_RPEAK * 4.0
        # DRAMBW = (bits/bytes) * Mem_freq * N channels * (Mega/Giga)
        self.arch_DRAMBW = (64./8.) * data['MEM_FREQ_MHZ'] * data['N_MEM_CHANNELS'] * (1e6/1e9) 
    


    def get_partition(self, data):

        node_type = re.search(r'([a-zA-Z]*)',data['NODENAME'][0])[0]
        node_number = int(re.search(r'(\d+)',data['NODENAME'][0])[0])

        if (node_type == "tcn") & (node_number <= 525):
            data['Arch'] = "AMD Rome 7H12 (2x)"
        elif (node_type == "tcn") & (node_number > 525):
            data['Arch'] = "AMD Genoa 9654 (2x)"
        elif (node_type == "gcn") & (node_number <= 72):
            data['Arch'] = "Intel Xeon Platinum 8360Y (2x)"
        elif (node_type == "tcn") & (node_number > 72):
            data['Arch'] = "AMD EPYC 9334 32-Core Processor (2x)"
        else:
            data['Arch'] = "UNK"

        return(data)


    def print_architecture_specs(self):

        with open(self.arch_spec_file) as csvfile:
            reader = csv.DictReader(csvfile, delimiter=";")
            for row in reader:
                for key in row:
                    print(key+":",row[key])
                print("DP RPEAK:",round(float(row['NCORES']) * float(row['CPU_FREQ_GHZ']) * float(row['NDPS']),2))
                print("SP RPEAK:",round(float(row['NCORES']) * float(row['CPU_FREQ_GHZ']) * float(row['NDPS']),2)*2.0)
                print("HP RPEAK:",round(float(row['NCORES']) * float(row['CPU_FREQ_GHZ']) * float(row['NDPS']),2)*4.0)
                print("DRAM BW:",round((64./8.) * float(row['MEM_FREQ_MHZ']) * float(row['N_MEM_CHANNELS']) * (1e6/1e9),2))
                print("-------------------")


    def print_timeline_metrics(self):
        print(self.loop_metrics)

    def csv_reader(self):

        header_list = []
        values_list = []
        idx = 0
        with open(self.filename) as csvfile:
            reader = csv.reader(csvfile, delimiter=";")
            for row in reader:
                if idx == 0:
                    header_list = row
                    idx += 1 # maybe there is better logic here
                    for i in range(len(header_list)):
                        values_list.append([])
                    continue
                else:
                    for i in range(len(row)):
                        try:
                            values_list[i].append(float(row[i]))
                        except:
                            values_list[i].append(row[i])
                                
        tmp_data = {}
        for column in header_list:
            
            tmp_data[column] = values_list[header_list.index(column)]
        
        return(tmp_data)
            

    def get_eacct_jobavg(self):

        self.filename = self.job_id+"."+self.step_id+'.csv'
        try:
            os.remove(self.filename)
        except FileNotFoundError:
            pass

        process = Popen(['eacct','-j',self.job_id+"."+self.step_id,'-l','-c',self.filename], stdout=PIPE, stderr=PIPE)

        output, error = process.communicate()
        output = output.decode('ISO-8859-1').strip()
        error = error.decode('ISO-8859-1').strip()

        if "No jobs found" in str(output):
            print(output)
            exit(1)
        
        tmp_data = self.csv_reader()
        tmp_data = self.get_partition(tmp_data)
        tmp_data = self.get_architecture_specs(tmp_data)

        tmp_data['OI'] = [tmp_data['CPU-GFLOPS'][0]/tmp_data['MEM_GBS'][0]]
        
        self.avgdata = tmp_data
        os.remove(self.filename) 


    def get_eacct_jobloop(self):

        self.filename = self.job_id+"."+self.step_id+'.csv'
        try:
            os.remove(self.filename)
        except FileNotFoundError:
            pass

        process = Popen(['eacct','-j',self.job_id+"."+self.step_id,'-r','-c',self.filename], stdout=PIPE, stderr=PIPE)

        output, error = process.communicate()
        output = output.decode('ISO-8859-1').strip()
        error = error.decode('ISO-8859-1').strip()


        if "No loops retrieved" not in error:
            tmp_data = self.csv_reader()
            self.loopdata = tmp_data
            self.loops_status = True
            os.remove(self.filename)
        else:
            print(error)
                

    def terminal(self, metrics):
        xmax = 10000 # this arbtrary

        # get other Precisions
        # Work Calculated on a node basis
        #NO_SIMD_DP_W = np.linspace(1./(Ncores*10),1.0,1000)*NO_SIMD_DP_Rpeak # thousand is just some arbitraty factor to make the line
        D_W = np.geomspace(1./(self.arch_ncores*100),1.0,100) *self.arch_DP_RPEAK # this is calculated for the full node
        S_W = np.geomspace(1./(self.arch_ncores*100),1.0,100) *self.arch_SP_RPEAK # this is calculated for the full node
        H_W = np.geomspace(1./(self.arch_ncores*100),1.0,100) *self.arch_HP_RPEAK # this is calculated for the full node
        
        # Operation Intensity (Flops/Byte)
        #NO_SIMD_DP_I = NO_SIMD_DP_W/DRAMBW
        D_I = D_W/self.arch_DRAMBW
        S_I = S_W/self.arch_DRAMBW
        H_I = H_W/self.arch_DRAMBW

        plx.clf()
        plx.subplots(1, 2)
        plx.subplot(1, 1)
        plx.theme("pro")
        plx.plotsize(70, 100)
        plx.xscale('log')
        plx.yscale('log')


        # Main Memory Line
        plx.scatter(H_I,H_W,color='white', marker="dot")

        # CPU Bound Lines
        #plx.plot(np.logspace(np.max(NO_SIMD_DP_I),5e10,len(NO_SIMD_DP_I)),NO_SIMD_DP_Rpeak*np.ones(len(NO_SIMD_DP_I)), c='white', ls = "--")
        plx.scatter(np.geomspace(np.max(D_I)/4.0,xmax,len(D_I)),self.arch_DP_RPEAK*np.ones(len(D_W))/4.0, color='white', marker="dot")
        plx.scatter(np.geomspace(np.max(D_I),xmax,len(D_I)),self.arch_DP_RPEAK*np.ones(len(D_W)), color='white', marker="dot")
        plx.scatter(np.geomspace(np.max(S_I),xmax,len(S_I)),self.arch_SP_RPEAK*np.ones(len(S_W)), color='white', marker="dot")
        plx.scatter(np.geomspace(np.max(H_I),xmax,len(H_I)),self.arch_HP_RPEAK*np.ones(len(H_W)), color='white', marker="dot")
        
        plx.text("HP", np.max(H_I) - np.max(H_I)*0.6, y = np.max(H_W))
        plx.text("SP", np.max(S_I) - np.max(S_I)*0.6, y = np.max(S_W))
        plx.text("DP", np.max(D_I) - np.max(D_I)*0.6, y = np.max(D_W))
        plx.text("DP 1/4 Node", np.max(D_I)/4.0 - np.max(D_I)/4.0 *0.9, y = np.max(D_W)/4.0)
        #plx.text("DRAM BW = "+ str(self.arch_DRAMBW)+ " GB/s", x = np.min(H_I), y = np.mean(H_W) + np.mean(H_W)*0.2)

        plx.title(self.arch_name + "  - DRAM BW = "+ str(self.arch_DRAMBW)+ " GB/s")

        #pdb.set_trace()
        plx.plot(self.avgdata["OI"], self.avgdata["CPU-GFLOPS"], color="red",marker='sd',label="JID: " + str(int(self.avgdata["JOBID"][0])) + "." +str(int(self.avgdata["STEPID"][0])))


        #plx.text(x = np.max(H_I)+ 600, y= np.max(NO_SIMD_DP_W) + np.max(NO_SIMD_DP_W) * Ytext_factor, text = "NO SIMD DP = "+ str(round(NO_SIMD_DP_Rpeak,2))+" GFLOPS", fontsize=8)
        plx.ylabel("Performance (GFLOPS)")
        plx.xlabel("Operational Intensity (FLOPS/byte)")
        plx.subplot(1,2)
        plx.theme('pro')

        if self.loops_status:

            plx.subplot(1,2).subplots(len(metrics), 1)
            
            for metric in metrics:
                plot_index = metrics.index(metric) 
                plx.subplot(1,2).subplot(plot_index +1 , 1)
                plx.scatter(self.loopdata["TIMESTAMP"], self.loopdata[metric],color='red',marker='dot')
                plx.ylabel(metric)
                
                #plx.ylim(0,1)
            plx.xlabel("Time (s)")

        else:
            plx.text("EAR LOOPS NOT ACTIVATED.\nRe-run your job with `export EARL_REPORT_LOOPS=1`",x=-0.5,y=0)
            plx.xlim(-1,1)
        plx.show()








        
        
