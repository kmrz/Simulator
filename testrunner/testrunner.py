#!/usr/bin/env python2
# -*- coding: utf-8 -*-
import argparse
import glob
import itertools
import logging
import multiprocessing
import os
import shutil
import subprocess


log = logging.getLogger(__name__)

simconfig = """
--title          %s
--job_id         0
--block_time     %d
--block_margin   24
--one_block      %s
--block_number   %d
--serial         %d
--cpu_count      %d
--cpu_percent    %d
--output         testrunner_results
--time_factor    %.2f

--threshold      %d
--decay          1
--default_limit  7
--share_file     shares.txt
--bf_depth       300
--bf_window      24
--bf_interval    5

--estimator      %s
--last_completed 2
--submitter      %s
--selector       VirtualSelector
--schedulers     %s
--share          EqualShare
"""

configdir = "testrunner_confs"

def conf(tracename, blocknumber = -1, time_factor = 1.0, clairvoyant = False, cpu_count = 0, serial = 0, threshold = 10, writefile = True):
    confname = "conf-trun-%s_%.2f-" % (tracename, time_factor)
    if clairvoyant:
        algs = "OStrich Fairshare"
        submitter = "OracleSubmitter"
        estimator = "NaiveEstimator"
        confname += "clairvoyant-"
    else:
        algs = "OStrich Fairshare"
        submitter = "FromWorkloadSubmitter"
        estimator = "PreviousNEstimator"
        confname += "nonclairvoyant-"
    if blocknumber == -1:
        one_block = "False"
        blocknumber = 0
        block_time = 0
        if cpu_count == 0:
            cpu_percent = 70
        else:
            cpu_percent = 0
        confname += "0"
    else:
        one_block = "True"
        block_time = int(90 * time_factor)
        confname += "%02d" % blocknumber
        cpu_percent = 0

    if writefile:
        curr_config = simconfig % (confname, block_time, one_block, blocknumber, serial, cpu_count, cpu_percent, time_factor, threshold, estimator, submitter, algs)
        with open(configdir+"/"+confname, "w") as f:
            f.write(curr_config)
    return confname


def run(*command):
    log.info("running: \n"+" ".join(command))
    returncode = subprocess.call(command)
    if returncode != 0:
        raise RuntimeError("Command: %s returned non-zero exit code %d" % (str(command), returncode))


def run_standard((argtrace, argconfig)):
    run("python", "main.py", "run", argtrace, argconfig)


def draw_trace(tracename, time_factor):
    s_time_factor = "%.2f" % time_factor
    fs_clairfile = glob.glob("testrunner_results/conf-trun-"+tracename+"_"+s_time_factor+"-clairvoyant-*-Fairshare-*")[0]
    fs_unclairfile = glob.glob("testrunner_results/conf-trun-"+tracename+"_"+s_time_factor+"-nonclairvoyant-*-Fairshare-*")[0]
    os_clairfile = glob.glob("testrunner_results/conf-trun-"+tracename+"_"+s_time_factor+"-clairvoyant-*-OStrich-*")[0]
    os_unclairfile = glob.glob("testrunner_results/conf-trun-"+tracename+"_"+s_time_factor+"-nonclairvoyant-*-OStrich-*")[0]
    log.info("files: %s %s %s %s", os_clairfile, fs_clairfile, os_unclairfile, fs_unclairfile)
    run("python", "drawing/draw_graphs.py", "--output", "testrunner_results/"+tracename, "--bw", "--striplegend", "--minlen", "60", os_clairfile, fs_clairfile, os_unclairfile, fs_unclairfile)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='run simulations on many logs; draw results')
    parser.add_argument('--genconfigs', action="store_true", help="generate config files")
    parser.add_argument('--simulate', action="store_true", help="run simulations")
    parser.add_argument('--workersize', type=int, default = 1, help="number of simulation workers")
    parser.add_argument('--workerrank', type=int, default = 0, help="rank of this worker")
    parser.add_argument('--draw', action="store_true", help="draw plots from results")
    args = parser.parse_args()
    traces = ["ANL-Intrepid-2009-1", "METACENTRUM-2009-2fil", "RICC-2010-2fil", "PIK-IPLEX-2009-1", "CEA-Curie-2011-2.1clnfil", "LLNL-Thunder-2007-1.1cln"]
    blocks = [1, 1, 1, 14, 3, 2] # number of 90-day blocks that will be independent simulations
    cpucounts = [163840, 375, 8192, 2560, 92260, 4096] # number of CPUs of a simulated machine
    serials = [163840/10, 368/4, 8192/10, 2560/10, 92260/10, 4096/10] # max number of processors a job can request
    thresholds = [10, 10, 1, 10, 10, 10]

    time_factors = [0.8, 0.85, 0.9, 0.95]

    # traces = ["RICC-2010-2"]
    # blocks = [1]
    # cpucounts = [7606]
    # serials = [7606/10]
    # traces = ["METAfil3cut"]
    # blocks = [1]
    # cpucounts = [ 375 ]
    # serials = [ 375 ]

    if not (args.genconfigs or args.simulate or args.draw):
        parser.error('No action requested, add --genconfigs or --simulate or --draw')

    try:
        os.mkdir("logs")
    except OSError:
        pass

    logging.basicConfig(level=logging.INFO, filename="logs/testrunner-%d.log" % (args.workerrank), filemode='w')
    log.info("args: "+str(args))

    if args.simulate or args.genconfigs:
        try:
            os.mkdir("testrunner_results")
        except OSError:
            pass
        if args.genconfigs:
            try:
                shutil.rmtree(configdir)
            except OSError:
                pass
            try:
                os.mkdir(configdir)
            except OSError:
                pass

        configs = []
        argtraces = []
        for time_factor in time_factors:
            for (i,tracename) in enumerate(traces):
                for blocknumber in range(0, blocks[i]):
                    if blocks[i] == 1:
                        blocknumber = -1
                    configs.append( conf(tracename, blocknumber, time_factor, True, cpucounts[i], serials[i], thresholds[i], args.genconfigs) )
                    configs.append( conf(tracename, blocknumber, time_factor, False, cpucounts[i], serials[i], thresholds[i], args.genconfigs) )
                    argtraces.append(tracename)
                    argtraces.append(tracename)

        argconfigs = [ "@"+configdir+"/"+config for config in configs ]
        argtraces = [ "../traces/"+tracename+".swf" for tracename in argtraces ]

        argconfigs = argconfigs[args.workerrank::args.workersize]
        argtraces = argtraces[args.workerrank::args.workersize]

        log.info("configs: "+str(argconfigs))
        log.info("traces: "+str(argtraces))

        if args.simulate:
            pool = multiprocessing.Pool(3)
            pool.map(run_standard, itertools.izip(argtraces, argconfigs))

    if args.draw:
        for time_factor in time_factors:
            for trace in traces:
                log.info("drawing trace: "+str(trace))
                draw_trace(trace, time_factor)
