import csv
from subprocess import Popen, PIPE
import os

from eacctview.dataloader import Dataloader
import numpy as np
import plotext as plx


class Plotter(Dataloader):
    def __init__(self):
        super().__init__()  # Call parent __init__

        self.plot_earl_off = True
        self.plot_earl_avg = True
        self.plot_earl_loops = True
        
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
        self.arch_palette = {
            "Rome": "tab:blue",
            "Genoa": "tab:orange",
            "A100": "tab:green",
            "H100": "tab:red",
            "Fat_Rome": "tab:purple",
            "Fat_Genoa": "tab:brown"
            }

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

    def get_metric_lims(self,metric):

        if metric == "CPI":
            min = 0.0
            max = 2.0
        elif metric == "MEM_GBS":
            min = 0.0
            max = self.arch_DRAMBW
        elif metric == "PERC_MPI":
            min = 0
            max = 100
        elif metric == "DC_NODE_POWER_W":
            min = 0
            max = self.arch_power + self.arch_power*0.1
        elif metric == "PCK_POWER_W":
            min = 0
            max = self.arch_power
        elif "PERC" in metric:
            min = 0
            max = 100
        elif "FREQ_KHZ" in metric:
            min = 0
            max = self.arch_freq + self.arch_freq * 0.1

        else:
            min = None
            max= None
        
        return(min,max)


    def var_vs_var(self,plx,vars):
            xvar =  vars[0]
            yvar =  vars[1]
            plx.theme("pro")

            for jobid in self.avgdata.keys():
                plx.plot(self.avgdata[jobid][xvar], self.avgdata[jobid][yvar],  marker='sd',label="JID: " + str(int(self.avgdata[jobid]["JOBID"][0])) +"."+ str(int(self.avgdata[jobid]["STEPID"][0])))
            plx.ylabel(yvar)
            plx.xlabel(xvar)

            xmin,xmax = self.get_metric_lims(xvar)
            ymin,ymax = self.get_metric_lims(yvar)
            
            if (ymin != None) and (ymax !=None):
                plx.ylim(ymin,ymax)
            else:
                plx.ylim(0,100)     
            if (xmin != None) and (xmax !=None):
                plx.xlim(xmin,xmax)
            else:
                plx.xlim(0,100)     


    def roofline(self,plx):

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

        plx.theme("pro")
        plx.xscale('log')
        plx.yscale('log')

        # Main Memory Line
        plx.scatter(H_I,H_W,color='white', marker="dot")

        # CPU Bound Lines
        plx.scatter(np.geomspace(np.max(D_I)/4.0,xmax,len(D_I)),self.arch_DP_RPEAK*np.ones(len(D_W))/4.0, color='white', marker="dot")
        plx.scatter(np.geomspace(np.max(D_I),xmax,len(D_I)),self.arch_DP_RPEAK*np.ones(len(D_W)), color='white', marker="dot")
        plx.scatter(np.geomspace(np.max(S_I),xmax,len(S_I)),self.arch_SP_RPEAK*np.ones(len(S_W)), color='white', marker="dot")
        plx.scatter(np.geomspace(np.max(H_I),xmax,len(H_I)),self.arch_HP_RPEAK*np.ones(len(H_W)), color='white', marker="dot")
        plx.text("HP", np.max(H_I) - np.max(H_I)*0.6, y = np.max(H_W))
        plx.text("SP", np.max(S_I) - np.max(S_I)*0.6, y = np.max(S_W))
        plx.text("DP", np.max(D_I) - np.max(D_I)*0.6, y = np.max(D_W))
        plx.text("DP 1/4 Node", np.max(D_I)/4.0 - np.max(D_I)/4.0 *0.9, y = np.max(D_W)/4.0)
        
        for jobid in self.avgdata.keys():
            plx.plot(self.avgdata[jobid]["OI"], self.avgdata[jobid]["CPU-GFLOPS"],marker='sd',label="JID: " + str(int(self.avgdata[jobid]["JOBID"][0])) +"."+ str(int(self.avgdata[jobid]["STEPID"][0])))

        plx.title(self.arch_name + "  - DRAM BW = "+ str(self.arch_DRAMBW)+ " GB/s")
        plx.ylabel("Performance (GFLOPS)")
        plx.xlabel("Operational Intensity (FLOPS/byte)")

    def timelines(self,plx,metrics):

        plx.subplot(1,2).subplots(len(metrics), 1)
        plx.theme("pro")

        for metric in metrics:
            for jobid in self.loopdata.keys():
                # Create a NumPy array of raw seconds
                seconds_array = self.loopdata[jobid]["TIMESTAMP"] - np.min(self.loopdata[jobid]["TIMESTAMP"]) 
            
                plot_index = metrics.index(metric) 
                plx.subplot(1,2).subplot(plot_index +1 , 1)
                plx.scatter(seconds_array, self.loopdata[jobid][metric],marker='dot')
            
                ymin,ymax = self.get_metric_lims(metric)
                if (ymin != None) and (ymax !=None):
                    plx.ylim(ymin,ymax)

                plx.ylabel(metric)
            
            
        plx.xlabel("Time (s)")


    def energy_bar(self):
        plx.theme("pro")
        plx.title("Energy(kWh)")
        energys = []
        jobids = []
        for jobid in self.avgdata.keys():
            avg_power = sum(self.avgdata[jobid]['DC_NODE_POWER_W'])/len(self.avgdata[jobid]['DC_NODE_POWER_W'])
            start = sum(self.avgdata[jobid]['START_TIME'])/len(self.avgdata[jobid]['START_TIME'])
            end = sum(self.avgdata[jobid]['END_TIME'])/len(self.avgdata[jobid]['END_TIME'])
            wtime = end - start
            jobids.append(jobid)
            # The reason appending each item as its own list is because of the multiple bar plot
            energys.append([avg_power * wtime * 2.77778e-7])

        plx.multiple_bar(["Jobs"], energys, label = jobids)
    
    def _sanity_plot(self):

        N_avg_jobs = len(self.avgdata.keys())
        N_loop_jobs = len(self.loopdata.keys())
        N_avg_fails = 0
        N_loop_fails = 0


        for jobid in list(self.avgdata.keys()):
            if self.avgdata[jobid]['EARL_AVG_ERR'].split():
                N_avg_fails += 1
                avg_err_msg = self.avgdata[jobid]['EARL_AVG_ERR']
                dummy = self.avgdata.pop(jobid, None)  # safely remove 'b'

        for jobid in list(self.loopdata.keys()):
            if not self.loopdata[jobid]:
                dummy = self.loopdata.pop(jobid, None)  # safely remove 'b'
                N_loop_fails += 1
        
        if N_avg_fails == N_avg_jobs:
            plx.subplot(1,1)#.subplot(2, 1)
            plx.theme("pro")
            plx.text(avg_err_msg,x=-0.75,y=0)
            plx.xlim(-1,1)
            self.plot_earl_avg = False
        if N_loop_fails == N_loop_jobs:
            plx.subplot(1,2)
            plx.theme("pro")
            plx.text("EAR LOOPS NOT ACTIVATED.\nRe-run your job with `export EARL_REPORT_LOOPS=1`",x=-0.5,y=0)
            plx.xlim(-1,1)
            self.plot_earl_loops = False


    def terminal(self, metrics, xvy_metrics=None):

        plx.clf()
        plx.subplots(1, 2)

        self._sanity_plot()

        plx.subplot(1,1).subplots(2, 1) 
        plx.plotsize(60, 100)

        # left panel
        plx.subplot(1,1).subplot(1, 1) # this should be the bar plot
        plx.plotsize(60,10)
        
        if self.plot_earl_avg:
            self.energy_bar()

        plx.subplot(1,1).subplot(2, 1) # this should be the roofline or xvy
        plx.theme("pro")

        if self.plot_earl_avg:
            if not xvy_metrics:
                self.roofline(plx)
            else:
                self.var_vs_var(plx, xvy_metrics)

        if self.plot_earl_loops:
            self.timelines(plx,metrics)

        plx.show()








        
        
