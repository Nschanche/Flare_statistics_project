import multiprocessing
import os
import numpy as np
import pandas as pd
from datetime import datetime
from datetime import timedelta
from sunpy.net import hek
import cPickle
import shelve
import gdbm
import sets
import sys
import signal

call_count = 0
# The @dview.parallel decorator turns a function into one that is
# automatically parallelized by IPython. Its arguments are split and sent to
# multiple engines, and its return values can be combined into a single value.

#@dview.parallel(block=True)
def query_hek(hinode_event):
    global call_count
    """Queries HEK for a hinode flare matching given parameters."""
    #import numpy as np
    #import pandas as pd
    #from datetime import datetime
    #from datetime import timedelta
    #from sunpy.net import hek
    
    
    # Error margin for peak times.
    #delta_T = timedelta(minutes=15)

    # HEK client.
    client = hek.HEKClient()
    
    #start_time = pd.to_datetime(hinode_event["peak"]) - delta_T
    start_time = pd.to_datetime(hinode_event["GOES start"])
    #print start_time
    #end_time = start_time + 2 * delta_T
    end_time = pd.to_datetime(hinode_event["end"])
    results = {"94": [], "131": [], "171": [], "211": [],}
    
    try:
        
        # Query parameters:
        #   - Time range
        #   - FLARE event type
        #   - Only choose Flare Detective triggers
        #TODO: remove this printout. It is unnecessary (lets you know that the 
        #program is running when the file is interpreted via command line.)
        print("Process %d submitting query. Events done so far: %d" %(os.getpid(), call_count))
        #print 'check1'
        hek_events = client.query(hek.attrs.Time(start_time.isoformat(),
                                                 end_time.isoformat()),
                                  hek.attrs.EventType('FL'),
                                  hek.attrs.FRM.Name == 'Flare Detective - Trigger Module')
        #print 'check2'
        print len(hek_events)
        
        call_count += 1
        
    except Exception as _ :
        return (hinode_event['# event No.'], results)

    for hek_event in hek_events:
        #print "got an event"
        if hek_event["obs_channelid"] not in results.keys():
            continue

        # Calculate the Euclidean distance between the Hinode event and the HEK event.
        hek_event["distance"] = np.sqrt(
            (hinode_event["event loc. (long.)"] - hek_event["event_coord1"])**2 +
            (hinode_event["event loc. (lat.)"] - hek_event["event_coord2"])**2)

        # Calculate the time difference between the Hinode event and the HEK event.
        #if pd.to_datetime(hinode_event['GOES start']) <= pd.to_datetime(hek_event["event_peaktime"]) <= pd.to_datetime(hinode_event["end"]):
        #    print "timerange is good"
        #else: 
        #    print "Nope, time is no good."
        #delta = (pd.to_datetime(hinode_event["peak"]) -
        #         pd.to_datetime(hek_event["event_peaktime"]))
        #hek_event["time_diff"] = delta.total_seconds()
        #results[hek_event["obs_channelid"]].append(hek_event)
        #print hinode_event["# event No."]
        if pd.to_datetime(hinode_event['GOES start']) <= pd.to_datetime(hek_event["event_peaktime"]) <= pd.to_datetime(hinode_event["end"]):
            delta = (pd.to_datetime(hinode_event["peak"]) -
                     pd.to_datetime(hek_event["event_peaktime"]))
            hek_event["time_diff"] = delta.total_seconds()
        else: 
            hek_event["time_diff"] = float("Inf")


        results[hek_event["obs_channelid"]].append(hek_event)
    return (hinode_event['# event No.'], results)


if __name__ == "__main__":
    """Queries HEK for a list of events, caching the results in a shelf.

    Returns a list of tuples, where the first element of a tuple is the
    hinode event number and the second is a list of HEK observations.
    """
    pool = multiprocessing.Pool(processes=25)
    hinode_events = cPickle.load(open('parallel_input.pkl', 'rb'))

    # Filter out the events we have already queried HEK for.
    # Note that the shelf does not allow integer keys.
    shelf = shelve.Shelf(gdbm.open(os.path.join(os.getcwd(),
                                                "hek_final.shelf"), "cu"))

    # We use a hash-based container since lookups are slow on regular python
    # lists.
    shelf_keys = sets.ImmutableSet(shelf.keys())
    
    uncached = []
    for event in hinode_events:
        
        if str(event["# event No."]) not in shelf_keys:
            uncached.append(event)
    print(len(uncached)) 
    
    def sigterm_handler(*args):
        global shelf
        print("SIGTERM received.")
        shelf.close()
        sys.exit(0)
    signal.signal(signal.SIGTERM, sigterm_handler)
   
    # Query HEK and cache the results.
    if uncached:
        
        uncached_results = pool.map(query_hek, uncached)
        
        for event_no, results in uncached_results:
            #shelf[event_no] = results
            shelf[str(event_no)] = results

    # Return results for the given hinode_events.
    results = []
    for event in hinode_events:
        event_no = event["# event No."]
        results.append((event_no, shelf[str(event_no)]))
    shelf.close()