#!/usr/bin/env python2
# -*- coding: utf-8 -*-
import subprocess
import os
import glob
import logging
import multiprocessing
import functools

simconfig = """
--title          %s
--job_id         0
--block_time     0
--block_margin   0
--one_block      False
--serial         0
--cpu_count      0
--cpu_percent    70
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


def conf_clairvoyant(tracename):
    curr_config = simconfig % ("trun-clairvoyant-"+tracename, "NaiveEstimator", "OracleSubmitter", "OStrich Fairshare")
    with open("clairvoyant_conf_"+tracename, "w") as f:
        f.write(curr_config)
    return "clairvoyant_conf_"+tracename

def conf_nonclairvoyant(tracename):
    curr_config = simconfig % ("trun-nonclairvoyant-"+tracename, "PreviousNEstimator", "FromWorkloadSubmitter", "OStrich")
    with open("nonclairvoyant_conf_"+tracename, "w") as f:
        f.write(curr_config)
    return "nonclairvoyant_conf_"+tracename

def run(*command):
    logging.info("running: \n"+" ".join(command))
    returncode = subprocess.call(command)
    if returncode != 0:
        raise RuntimeError("Command: %s returned non-zero exit code %d" % (str(command), returncode))

def test_trace(tracename, tracedir=""):
    cc = conf_clairvoyant(tracename)
    cnc = conf_nonclairvoyant(tracename)
    run("python", "main.py", "run", tracedir+tracename+".swf", "@"+cc)
    run("python", "main.py", "run", tracedir+tracename+".swf", "@"+cnc)

def draw_trace(tracename):
    fsfile = glob.glob("testrunner_results/trun-clairvoyant-"+tracename+"-Fairshare-*")[0]
    logging.info("fairshare file: %s" % fsfile)
    clairfile = glob.glob("testrunner_results/trun-clairvoyant-"+tracename+"-OStrich-*")[0]
    logging.info("clairvoyant ostrich file: %s" % clairfile)
    unclairfile = glob.glob("testrunner_results/trun-nonclairvoyant-"+tracename+"-OStrich-*")[0]
    logging.info("non-clairvoyant ostrich file: %s" % unclairfile)
    run("python", "drawing/draw_graphs.py", "--output", "testrunner_results/"+tracename, "--minlen", "30", fsfile, clairfile, unclairfile)

if __name__ == "__main__":
    try:
        os.mkdir("testrunner_results")
    except OSError:
        pass
    logging.basicConfig(level=logging.INFO)

    traces = ["ANL-Intrepid-2009-1", "CEA-Curie-2011-2", "METACENTRUM-2009-2", "RICC-2010-2", "PIK-IPLEX-2009-1", ]
    tracedir = "../traces/"
    pool = multiprocessing.Pool(multiprocessing.cpu_count() / 2)
    pool.map(functools.partial(test_trace, tracedir=tracedir), traces)
    # for trace in traces:
    #     draw_trace(trace)
