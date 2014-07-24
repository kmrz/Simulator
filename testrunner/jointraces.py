import os
import glob

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
            blockindex = blockname.split("-")[7]
            if int(blockindex) == block_count:
                break
            with open(blockname, "r") as input:
                for line in input:
                    output.write(line)

if __name__=="__main__":
    logging.basicConfig(level=logging.INFO)
    traces = ["PIK-IPLEX-2009-1", "CEA-Curie-2011-2", ]
    blocks = [14, 8]
    for (block_count, trace) in zip(blocks, traces):
        for alg in ("Fairshare", "OStrich"):
            for voyance in ("clairvoyant", "nonclairvoyant"):
                if alg=="Fairshare" and voyance=="nonclairvoyant":
                    continue
                tracename = "testrunner_results/conf-trun-"+voyance+"-"+trace+"*"+alg+"*"
                outname = "testrunner_results/conf-trun-"+voyance+"-"+trace+"-00-b-"+alg
                jointrace(tracename, outname, block_count)
