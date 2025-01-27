import argparse
import csv
import re
from subprocess import Popen, PIPE
import os

import pdb
import numpy as np
import plotext as plx
import pandas as pd


class Plotter():
    def __init__(self):

        self.avgdata = {}
        self.loopdata = {}

        self.loops_status=False
        self.loop_metrics = ["AVG_CPUFREQ_KHZ","AVG_IMCFREQ_KHZ","DEF_FREQ_KHZ","CPI","TPI","MEM_GBS","IO_MBS","PERC_MPI","DC_NODE_POWER_W","DRAM_POWER_W","PCK_POWER_W","GFLOPS","L1_MISSES","L2_MISSES","L3_MISSES","SPOPS_SINGLE","SPOPS_128","SPOPS_256","SPOPS_512","DPOPS_SINGLE","DPOPS_128","DPOPS_256","DPOPS_512","TEMP0","TEMP1","TEMP2","TEMP3","GPU0_POWER_W","GPU0_FREQ_KHZ","GPU0_MEM_FREQ_KHZ","GPU0_UTIL_PERC","GPU0_MEM_UTIL_PERC","GPU0_GFLOPS","GPU0_TEMP","GPU0_MEMTEMP","GPU1_POWER_W","GPU1_FREQ_KHZ","GPU1_MEM_FREQ_KHZ","GPU1_UTIL_PERC","GPU1_MEM_UTIL_PERC","GPU1_GFLOPS","GPU1_TEMP","GPU1_MEMTEMP","GPU2_POWER_W","GPU2_FREQ_KHZ","GPU2_MEM_FREQ_KHZ","GPU2_UTIL_PERC","GPU2_MEM_UTIL_PERC","GPU2_GFLOPS","GPU2_TEMP","GPU2_MEMTEMP","GPU3_POWER_W","GPU3_FREQ_KHZ","GPU3_MEM_FREQ_KHZ","GPU3_UTIL_PERC","GPU3_MEM_UTIL_PERC","GPU3_GFLOPS","GPU3_TEMP","GPU3_MEMTEMP","GPU4_POWER_W","GPU4_FREQ_KHZ","GPU4_MEM_FREQ_KHZ","GPU4_UTIL_PERC","GPU4_MEM_UTIL_PERC","GPU4_GFLOPS","GPU4_TEMP","GPU4_MEMTEMP","GPU5_POWER_W","GPU5_FREQ_KHZ","GPU5_MEM_FREQ_KHZ","GPU5_UTIL_PERC","GPU5_MEM_UTIL_PERC","GPU5_GFLOPS","GPU5_TEMP","GPU5_MEMTEMP","GPU6_POWER_W","GPU6_FREQ_KHZ","GPU6_MEM_FREQ_KHZ","GPU6_UTIL_PERC","GPU6_MEM_UTIL_PERC","GPU6_GFLOPS","GPU6_TEMP","GPU6_MEMTEMP","GPU7_POWER_W","GPU7_FREQ_KHZ","GPU7_MEM_FREQ_KHZ","GPU7_UTIL_PERC","GPU7_MEM_UTIL_PERC","GPU7_GFLOPS","GPU7_TEMP","GPU7_MEMTEMP","LOOP_SIZE"]

        self.filename = "tmp.csv"

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


    def get_architecture_specs(self, data):

        with open("data/architecture_specs.csv") as csvfile:
            reader = csv.DictReader(csvfile, delimiter=";")
            for row in reader:
                if row["NAME"] == data['Arch']:
                    for key in row.keys():
                        if key == "NAME":
                            # skip the NAME since it is already in Arch
                            continue
                        else:
                            data[key] = row[key]

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

        node_type = re.search(r'([a-zA-Z]*)',data['NODENAME'])[0]
        node_number = int(re.search(r'(\d+)',data['NODENAME'])[0])

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


    def print_timeline_metrics(self):
        print(self.loop_metrics)


    def get_eacct_jobavg(self,jobid,stepid = 0):

        self.filename = jobid+"."+str(stepid)+'.csv'
        try:
            os.remove(self.filename)
        except FileNotFoundError:
            pass

        process = Popen(['eacct','-j',jobid+"."+str(stepid),'-l','-c',self.filename], stdout=PIPE, stderr=PIPE)

        output, error = process.communicate()
        output = output.decode('ISO-8859-1').strip()
        error = error.decode('ISO-8859-1').strip()

        if "No jobs found" in str(output):
            print(output)
            exit(1)
        
        tmp_data = {} # this will only work for an average job
        with open(self.filename) as csvfile:
            reader = csv.DictReader(csvfile, delimiter=";")
            for row in reader:
                tmp_data = row
        
        tmp_data = self.get_partition(tmp_data)
        tmp_data = self.get_architecture_specs(tmp_data)

        # convert dictionary items to floats
        for k, v in tmp_data.items():
            try:
                tmp_data[k] = float(v)
            except:
                pass # some values will be strings
        
        self.set_architecture_specs(tmp_data)

        
        tmp_data['OI'] = tmp_data['CPU-GFLOPS']/tmp_data['MEM_GBS']
        
        self.avgdata = tmp_data


    def eacct_loop(self,jobid,stepid = 0):

        self.filename = jobid+"."+str(stepid)+'.csv'

        try:
            os.remove(self.filename)
        except FileNotFoundError:
            pass

        process = Popen(['eacct','-j',jobid+"."+str(stepid),'-r','-c',self.filename], stdout=PIPE, stderr=PIPE)

        output, error = process.communicate()
        output = output.decode('ISO-8859-1').strip()
        error = error.decode('ISO-8859-1').strip()
                
        try:
            tmp_data = pd.read_csv(self.filename,delimiter=";")
            tmp_data = self.get_partition(tmp_data)
            tmp_data['time'] = (pd.to_datetime(tmp_data['DATE']) - pd.to_datetime(tmp_data['DATE']).min()).dt.seconds
            self.loopdata = tmp_data
            self.loops_status = True
        except:
            if "No loops retrieved" in str(output):
                print(output)
            print("Loops not collected my EAR")
            self.loops_status = False


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
        plx.text("DP 1/4 Node", x = xmax - xmax*0.99, y = np.max(D_W)/4.0)

        plx.scatter(np.geomspace(np.max(D_I),xmax,len(D_I)),self.arch_DP_RPEAK*np.ones(len(D_W)), color='white', marker="dot")
        plx.text("DP Rpeak = "+ str(round(self.arch_DP_RPEAK,2)), x = xmax - xmax*0.99, y = np.max(D_W))

        plx.scatter(np.geomspace(np.max(S_I),xmax,len(S_I)),self.arch_SP_RPEAK*np.ones(len(S_W)), color='white', marker="dot")
        plx.text("SP Rpeak = "+ str(round(self.arch_SP_RPEAK,2)), x = xmax - xmax*0.99, y = np.max(S_W))
        
        plx.scatter(np.geomspace(np.max(H_I),xmax,len(H_I)),self.arch_HP_RPEAK*np.ones(len(H_W)), color='white', marker="dot")
        plx.text("HP Rpeak = "+ str(round(self.arch_HP_RPEAK,2)), x = xmax - xmax*0.99, y = np.max(H_W))

        plx.title(self.arch_name + "  - DRAM BW = "+ str(self.arch_DRAMBW)+ " GB/s")

        plx.plot([self.avgdata["OI"]], [self.avgdata["CPU-GFLOPS"]], color="red",marker='sd',label="JID: " + str(self.avgdata["JOBID"]))

        #plx.text(x = np.max(H_I)+ 600, y= np.max(NO_SIMD_DP_W) + np.max(NO_SIMD_DP_W) * Ytext_factor, text = "NO SIMD DP = "+ str(round(NO_SIMD_DP_Rpeak,2))+" GFLOPS", fontsize=8)
        plx.text("DRAM BW = "+ str(self.arch_DRAMBW)+ " GB/s", x = np.min(H_I), y = np.mean(H_W) + np.mean(H_W)*0.2)
        plx.ylabel("Performance (GFLOPS)")
        plx.xlabel("Operational Intensity (FLOPS/byte)")

        plx.subplot(1,2)
        plx.theme('pro')

        if self.loops_status:

            plx.subplot(1,2).subplots(len(metrics), 1)
            
            for metric in metrics:
                plot_index = metrics.index(metric) 
                plx.subplot(1,2).subplot(plot_index +1 , 1)
                plx.scatter(self.loopdata["time"], self.loopdata[metric],color='red',marker='dot')
                plx.ylabel(metric)
                
                #plx.ylim(0,1)
            plx.xlabel("Time (s)")

        else:
            plx.text("EAR LOOPS NOT ACTIVATED.\nRe-run your job with `export EARL_REPORT_LOOPS=1`",x=-0.5,y=0)
            plx.xlim(-1,1)
        plx.show()


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-j", "--jobid", metavar="JobID", help="Plot Roofline and Timeline from eacct tool", type=str, nargs=1)
    parser.add_argument("--metrics", metavar="Metrics", help="Metrics to plot timeline", default = ["CPI", "MEM_GBS", "GFLOPS"], type=str, nargs='+', required=False)
    parser.add_argument("--list", action='store_true', help="List available metrics to plot timeline", required=False)
    
    args = parser.parse_args()

    plotter = Plotter()

    if args.list:
        plotter.print_timeline_metrics()
        exit(1)
    if args.jobid:
        plotter.get_eacct_jobavg(args.jobid[0])
        plotter.eacct_loop(args.jobid[0])
        plotter.terminal(args.metrics)

    #plotter.roofline()
    #plotter.timeline()




        
        
