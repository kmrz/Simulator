#!/usr/bin/env python2
# -*- coding: utf-8 -*-
import subprocess
import os
import glob
import logging
import multiprocessing
import functools
import argparse
import itertools
import shutil

simconfig = """
--title          %s
--job_id         0
--block_time     %d
--block_margin   24
--one_block      %s
--block_number   %d
--serial         0
--cpu_count      %d
--cpu_percent    %d
--output         testrunner_results

--threshold      10
--decay          1
--default_limit  7
--share_file     shares.txt
--bf_depth       50
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

def conf(tracename, blocknumber = -1, clairvoyant = False, cpu_count = 0, writefile = True):
    confname = "conf-trun-"
    if clairvoyant:
        algs = "OStrich Fairshare"
        submitter = "OracleSubmitter"
        estimator = "NaiveEstimator"
        confname += "clairvoyant-"
    else:
        algs = "OStrich"
        submitter = "FromWorkloadSubmitter"
        estimator = "PreviousNEstimator"
        confname += "nonclairvoyant-"
    confname += tracename + "-"
    if blocknumber == -1:
        one_block = "False"
        blocknumber = 0
        block_time = 0
        cpu_count = 0
        cpu_percent = 70
        confname += "0"
    else:
        one_block = "True"
        block_time = 90
        confname += str(blocknumber)
        cpu_percent = 0

    if writefile:
        curr_config = simconfig % (confname, block_time, one_block, blocknumber, cpu_count, cpu_percent, estimator, submitter, algs)
        with open(configdir+"/"+confname, "w") as f:
            f.write(curr_config)
    return confname


def run(*command):
    logging.info("running: \n"+" ".join(command))
    returncode = subprocess.call(command)
    if returncode != 0:
        raise RuntimeError("Command: %s returned non-zero exit code %d" % (str(command), returncode))


def run_standard((argtrace, argconfig)):
    run("python", "main.py", "run", argtrace, argconfig)


def draw_trace(tracename):
    fsfile = glob.glob("testrunner_results/trun-clairvoyant-"+tracename+"-Fairshare-*")[0]
    logging.info("fairshare file: %s" % fsfile)
    clairfile = glob.glob("testrunner_results/trun-clairvoyant-"+tracename+"-OStrich-*")[0]
    logging.info("clairvoyant ostrich file: %s" % clairfile)
    unclairfile = glob.glob("testrunner_results/trun-nonclairvoyant-"+tracename+"-OStrich-*")[0]
    logging.info("non-clairvoyant ostrich file: %s" % unclairfile)
    run("python", "drawing/draw_graphs.py", "--output", "testrunner_results/"+tracename, "--bw", "--striplegend", "--minlen", "30", fsfile, clairfile, unclairfile)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='run simulations on many logs; draw results')
    parser.add_argument('--genconfigs', action="store_true", help="generate config files")
    parser.add_argument('--simulate', action="store_true", help="run simulations")
    parser.add_argument('--workersize', type=int, default = 1, help="number of simulation workers")
    parser.add_argument('--workerrank', type=int, default = 0, help="rank of this worker")
    parser.add_argument('--draw', action="store_true", help="draw plots from results")
    args = parser.parse_args()
    traces = ["ANL-Intrepid-2009-1", "METACENTRUM-2009-2", "RICC-2010-2", "PIK-IPLEX-2009-1", "CEA-Curie-2011-2", ]
    blocks = [1, 1, 1, 14, 7]
    cpucounts = [0, 0, 0, 1418, 33336]

    logging.basicConfig(level=logging.INFO, filename="testrunner.log")

    if args.simulate:
        try:
            os.mkdir("testrunner_results")
        except OSError:
            pass
        try:
            shutil.rmtree(configdir)
        except OSError:
            pass
        try:
            os.mkdir(configdir)
        except OSError:
            pass

        try:
            os.mkdir("logs")
        except OSError:
            pass

        configs = []
        argtraces = []
        for (i,tracename) in enumerate(traces):
            for blocknumber in range(0, blocks[i]):
                if blocks[i] == 1:
                    blocknumber = -1
                configs.append( conf(tracename, blocknumber, True, cpucounts[i], args.genconfigs) )
                configs.append( conf(tracename, blocknumber, False, cpucounts[i], args.genconfigs) )
                argtraces.append(tracename)
                argtraces.append(tracename)

        argconfigs = [ "@"+configdir+"/"+config for config in configs ]
        argtraces = [ "../traces/"+tracename+".swf" for tracename in argtraces ]

        argconfigs = argconfigs[args.workerrank::args.workersize]
        argtraces = argtraces[args.workerrank::args.workersize]

        logging.info("configs: "+str(argconfigs))
        logging.info("traces: "+str(argtraces))
        pool = multiprocessing.Pool(multiprocessing.cpu_count() / 2)
        pool.map(run_standard, itertools.izip(argtraces, argconfigs))

    # if args.draw:
    #     for trace in traces:
    #         draw_trace(trace)


