import logging
import csv
import bisect
import sys

log = logging.getLogger("allocator")

class Job(object):
    def __init__(self, *args):
        self.ID = args[0]
        self.submit = args[1]
        self.start = args[2]
        self.end = args[3]
        self.nprocs = args[4]
        self.allocprocs = [] # a list of ranges: (first allocated processor, last allocated processor)

    def __str__(self):
        return "%s: (%d, %d) procs %d alloc %s" % (self.ID, self.start, self.end, self.nprocs, self.allocprocs)

class Event(object):
    def __init__(self, time, evtype, job):
        self.time = time
        self.evtype = evtype
        self.job = job


    def __str__(self):
        return "%d: %s" % (self.time, self.job)


def parse_log(logfilename):
    events = []
    with open(logfilename, 'rb') as joblog:
        joblog.readline() # skip header
        jobreader = csv.reader(joblog, delimiter = ",")
        for line in jobreader:
            job = Job(line[2] + "-" + line[3] + "-" + line[1], int(line[4]), int(line[5]), int(line[5]) + int(line[6]), int(line[7]))
            start = Event(job.start, 1, job)
            end = Event(job.end, 0, job)
            events.append(start)
            events.append(end)
    events.sort(key = lambda event: (event.time, event.evtype))
    return events

def validate(time, proc_count, events, firsts, lasts):
    avail_procs = 0
    for (f,l) in zip(firsts, lasts):
        avail_procs += l - f + 1
    busy_procs = 0
    for event in events:
        if event.evtype == 0 and event.job.start < time and event.job.end >= time:
            for (f,l) in event.job.allocprocs:
                busy_procs += l - f + 1
    log.debug("time %d avail %d busy %d" % (time, avail_procs, busy_procs))
    assert avail_procs + busy_procs == proc_count, "processors are leaking! time %d avail %d busy %d sum %d missing %d" % (time, avail_procs, busy_procs, avail_procs + busy_procs, proc_count - avail_procs - busy_procs)


def allocate_procs(events, proc_count):
    firsts = [0]
    lasts = [proc_count - 1]
    last_event_time = -1

    for event in events:
        log.debug("processing event")
        log.debug(event)
        log.debug(zip(firsts, lasts))
        if event.time != last_event_time:
            validate(event.time, proc_count, events, firsts, lasts)
            last_event_time = event.time

        job = event.job
        if event.time == job.start:
            total_alloc = 0
            while total_alloc < job.nprocs:
                assert len(firsts) > 0, "time %d; missing %d processors for job %s" % (event.time, job.nprocs - total_alloc, job)
                bestfit = proc_count + 1
                bestind = -1
                for (i, (first, last)) in enumerate(zip(firsts, lasts)):
                   fit = abs(job.nprocs - ( last - first + 1))
                   if fit < bestfit:
                       bestfit = fit
                       bestind = i
                       if fit == 0:
                           break
                alloc = min(job.nprocs, lasts[bestind] - firsts[bestind] + 1)
                total_alloc += alloc
                job.allocprocs.append( (firsts[bestind], firsts[bestind] + alloc - 1) )
                if alloc == lasts[bestind] - firsts[bestind] + 1:
                    del firsts[bestind]
                    del lasts[bestind]
                else:
                    firsts[bestind] += alloc
        elif event.time == job.end:
            for (falloc, lalloc) in job.allocprocs:
                floc = bisect.bisect_left(firsts, falloc)
                if floc == 0:
                    if len(firsts) > 0 and firsts[0] == lalloc + 1:
                        firsts[0] = falloc
                    else:
                        firsts.insert(0, falloc)
                        lasts.insert(0, lalloc)
                elif floc == len(firsts):
                    if lasts[-1] + 1 == falloc:
                        lasts[-1] = lalloc
                    else:
                        firsts.insert(floc, falloc)
                        lasts.insert(floc, lalloc)
                else:
                    if lasts[floc - 1] +1 == falloc:
                        if firsts[floc] == lalloc + 1:
                            del lasts[floc - 1]
                            del firsts[floc]
                        else:
                            lasts[floc - 1] = lalloc
                    elif firsts[floc] == lalloc + 1:
                        firsts[floc] = falloc
                    else:
                        firsts.insert(floc, falloc)
                        lasts.insert(floc, lalloc)
        else:
            assert False, "Strange event %s" % event

        log.debug("after processing event")
        log.debug(event)
        log.debug(zip(firsts, lasts))


def export_jed(jobs, proc_count):
    header = """<?xml version="1.0"?>
<grid_schedule>
  <meta_info/>
  <grid_info>
    <info name="nb_clusters" value="1"/>
    <clusters>
      <cluster id="0" hosts="%d" first_host="0"/>
    </clusters>
    <info name="unit" value="seconds" />
    <info name="slots" value="processors" />
  </grid_info>
  <node_infos>""" % proc_count
    print header

    for job in jobs:
        jobheader = """<node_statistics>
      <node_property name="id" value="%s"/>
      <node_property name="type" value="computation"/>
      <node_property name="start_time" value="%d"/>
      <node_property name="end_time" value="%d"/>
      <configuration>
        <conf_property name="cluster_id" value="0"/>
        <conf_property name="host_nb" value="%d"/>

        <host_lists>""" % (job.ID, job.start, job.end, job.nprocs)
        print jobheader

        nprocs = 0
        for (falloc, ealloc) in job.allocprocs:
            hosts = """<hosts start="%d" nb="%d"/>""" % (falloc, ealloc - falloc + 1)
            nprocs += ealloc - falloc + 1
            print hosts
        assert nprocs == job.nprocs, "job has wrong number of procs allocated %s" % job

        print """        </host_lists>
        </configuration>
    </node_statistics>"""


    print """  </node_infos>
</grid_schedule>"""


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    events = parse_log(sys.argv[1])
    system_procs = int(sys.argv[2])
    allocate_procs(events, system_procs)
    jobs = ( event.job for event in events if event.evtype == 1)
    export_jed(jobs, system_procs)
