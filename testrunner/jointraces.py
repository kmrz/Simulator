import os
import glob
from time import strftime

import logging

log = logging.getLogger(__name__)

def jointrace(tracename, outname, block_count):
    try:
        os.remove(outname)
    except OSError:
        pass
    blocknames = glob.glob(tracename)
    blocknames.sort()
    log.info("tracename %s", tracename)
    with open(outname, "w") as output:
        for blockname in blocknames:
            log.info("blockname %s", blockname)
            blockindex = blockname.split("-")[-5]
            if int(blockindex) == block_count:
                break
            with open(blockname, "r") as input:
                for line in input:
                    output.write(line)

if __name__=="__main__":
    logging.basicConfig(level=logging.INFO)
    traces = ["PIK-IPLEX-2009-1", "CEA-Curie-2011-2.1cln", "LLNL-Thunder-2007-1.1cln"]
    blocks = [14, 3, 2]
    now = strftime("%b%d_%H-%M")
    for (block_count, trace) in zip(blocks, traces):
        for alg in ("Fairshare", "OStrich"):
            for voyance in ("clairvoyant", "nonclairvoyant"):
                tracename = "testrunner_results/blocktraces/conf-trun-"+trace+"-"+voyance+"*"+alg+"*"
                outname = "testrunner_results/conf-trun-"+trace+"-"+voyance+"-0-"+alg+"-"+now
                jointrace(tracename, outname, block_count)
