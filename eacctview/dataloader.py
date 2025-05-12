import csv
import re
from subprocess import Popen, PIPE
import os
import concurrent.futures

import numpy as np
import plotext as plx


class Dataloader():
    def __init__(self):

        self.userdata = {}
        self.avgdata = {}
        self.loopdata = {}

        self.job_ids = []

    def get_jobid(self,jobids_from_args):
        """
        gets the jobid/s (with corresponding stepid)
        Defaults step id = 0 if non is specified
        """
        for jobid in jobids_from_args:
            if len(jobid.split(".")) == 1:
                self.job_ids.append(jobid + ".0")
            elif len(jobid.split(".")) == 2:
                self.job_ids.append(jobid)

    def get_eacct_from_csv(self, filename):

        tmp_data = self._csv_reader(filename)
        tmp_data = self._get_partition(tmp_data)
        tmp_data = self._get_architecture_specs(tmp_data)


        # This is a bit of a hack
        # because csv can be created in multiple ways aparently
        if "LOOPID" in tmp_data.keys():
            self.loopdata = tmp_data
            tmp_data['OI'] = [sum(tmp_data['GFLOPS'])/len(tmp_data['GFLOPS'])/sum(tmp_data['MEM_GBS'])/len(tmp_data['MEM_GBS'])]
            tmp_data["CPU-GFLOPS"] = [sum(tmp_data['GFLOPS'])/len(tmp_data['GFLOPS'])] # again disparity in data
            self.avgdata = tmp_data
            self.loops_status = True

        else:
            tmp_data['OI'] = [tmp_data['CPU-GFLOPS'][0]/tmp_data['MEM_GBS'][0]]
            self.avgdata = tmp_data
    

    def get_eacct_data(self):

        # Maybe the use of threading is overkill.
        executor_avg = concurrent.futures.ThreadPoolExecutor(max_workers=len(self.job_ids))
        executor_loop = concurrent.futures.ThreadPoolExecutor(max_workers=len(self.job_ids))
        
        result_avg = executor_avg.map(self._get_eacct_jobavg, self.job_ids)
        result_loop = executor_loop.map(self._get_eacct_jobloop, self.job_ids)

        avg_data = list(result_avg)
        loop_data  = list(result_loop)
        for i in range(len(avg_data)):

            self.avgdata[self.job_ids[i]] = avg_data[i]
            self.loopdata[self.job_ids[i]] = loop_data[i]


    def _csv_reader(self,filename):

        header_list = []
        values_list = []
        idx = 0

        with open(filename) as csvfile:
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
        
        self._check_eacct_data(tmp_data)
        return(tmp_data)
    
    def _check_eacct_data(self,tmp_data):
        # check if policy is enabled

        if ('POLICY' in tmp_data.keys()):
            if not tmp_data['POLICY'][0].strip():
                err_msg = "Did not enable an EAR policy.\n"
                err_msg += "No Application metrics can be found/displayed.\n"
                err_msg += "Resubmit your job with:\n"
                err_msg += "#SBATCH --ear=on\n"
                err_msg += "#SBATCH --ear-policy=monitoring/min_time/min_energy\n"
                tmp_data["EARL_AVG_ERR"] = err_msg
                return tmp_data

        # check if actual metrics were found for the job
        for mem in tmp_data['MEM_GBS']:
            index = tmp_data['MEM_GBS'].index(mem)
            if (mem == 0.0) and (tmp_data['CPI'][index] == 0.0):
                err_msg = "EARL Enabled with a policy BUT no metrics found!!!\n"
                err_msg += "THIS IS WEIRD THERE SHOULD BE\n"
                err_msg += "Something went wrong with the job or DB\n"
                err_msg += "Check the command eacct -j JOBID to see whats going on.\n"
                tmp_data["EARL_AVG_ERR"] = err_msg
                return tmp_data
        tmp_data["EARL_AVG_ERR"] = ""
        return tmp_data


    def _get_eacct_basic(self):
        '''
        Get the energy of the last 5 jobs run.
        '''
        self.userfile = os.environ["USER"] + ".csv"

        try:
            print("Querying username: "+ os.environ["USER"])
            process = Popen(['eacct','-u',os.environ["USER"],'-n 10','-c',self.userfile], stdout=PIPE, stderr=PIPE)
    
            output, error = process.communicate()
            output = output.decode('ISO-8859-1').strip()
            error = error.decode('ISO-8859-1').strip()

        except FileNotFoundError:
            print("eacct command not found.")
            print("You need to load the ear module or install the eacct tool....")
            exit(1)        
        
        self.userdata = self._csv_reader(self.userfile)

    def _get_eacct_jobavg(self,jobid):
        '''
        get the average job statistics from eacct
        '''
        avgfile = jobid+'.avg.csv'
        try:
            print("Querying jobavg: (jobid.stepid): ("+jobid+")")
            process = Popen(['eacct','-j',jobid,'-l','-c',avgfile], stdout=PIPE, stderr=PIPE)
    
            output, error = process.communicate()
            output = output.decode('ISO-8859-1').strip()
            error = error.decode('ISO-8859-1').strip()
        except FileNotFoundError:
            err_msg = "eacct command not found.\n"
            err_msg += "You need to load the ear module or install the eacct tool...."

        tmp_data = {}
        if 'No jobs found' in error:
            err_msg = "Could not find job step from eacct.\n"
            err_msg += "You probably did not enable the EARL.\n"
            err_msg += "Check the command eacct -j JOBID..\nto see if there exists any job steps..\n"
            tmp_data["EARL_AVG_ERR"] = err_msg
        else:
            tmp_data = self._csv_reader(avgfile)
            tmp_data = self._get_partition(tmp_data)
            tmp_data = self._get_architecture_specs(tmp_data)
            tmp_data = self._check_eacct_data(tmp_data)
            try:
                tmp_data['OI'] = [tmp_data['CPU-GFLOPS'][0]/tmp_data['MEM_GBS'][0]]
            except:
                tmp_data['OI'] = 0 
            os.remove(avgfile) 
        
        return(tmp_data)


    def _get_eacct_jobloop(self,jobid):

        loopfile = jobid+'.loop.csv'
        try:
            os.remove(loopfile)
        except FileNotFoundError:
            pass

        print("Querying jobloop: (jobid.stepid): ("+jobid+")")
        process = Popen(['eacct','-j',jobid,'-r','-c',loopfile], stdout=PIPE, stderr=PIPE)
        output, error = process.communicate()
        output = output.decode('ISO-8859-1').strip()
        error = error.decode('ISO-8859-1').strip()

        if "No loops retrieved" not in error:
            tmp_data = self._csv_reader(loopfile)
            os.remove(loopfile)
            tmp_data["EARL_LOOP_ERR"] = ""
            return(tmp_data)
        else:
            tmp_data = {}
            tmp_data["EARL_LOOP_ERR"] = "No loops retrieved for jid "+jobid+"\n"
            tmp_data["EARL_LOOP_ERR"] += "Check to see if EARL_REPORT_LOOPS=1 was set in the Job\n"
        

    def _get_partition(self, data):

        node_type = re.search(r'([a-zA-Z]*)',data['NODENAME'][0])[0]
        node_number = int(re.search(r'(\d+)',data['NODENAME'][0])[0])

        if (node_type == "tcn") & (node_number <= 525):
            data['Arch'] = "AMD Rome 7H12 (2x)"
        elif (node_type == "tcn") & (node_number > 525):
            data['Arch'] = "AMD Genoa 9654 (2x)"
        elif (node_type == "gcn") & (node_number <= 72):
            data['Arch'] = "Intel Xeon Platinum 8360Y (2x)"
        elif (node_type == "gcn") & (node_number > 72):
            data['Arch'] = "AMD EPYC 9334 32-Core Processor (2x)"
        else:
            data['Arch'] = "UNK"

        return(data)
    
    def _get_architecture_specs(self, data):

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
                    self.arch_power = float(row["MAX_POWER_W"])
                    self.arch_DP_RPEAK = self.arch_ncores * self.arch_freq * self.arch_NDPs
                    self.arch_SP_RPEAK = self.arch_DP_RPEAK * 2.0
                    self.arch_HP_RPEAK = self.arch_DP_RPEAK * 4.0
                    # DRAMBW = (bits/bytes) * Mem_freq * N channels * (Mega/Giga)
                    self.arch_DRAMBW = (64./8.) * self.arch_memory_freq * self.arch_memory_channels * (1e6/1e9) 

        return(data)

