import pymongo
import os
import glob
import time
import hax
import datetime
from multiprocessing.dummy import Pool as ThreadPool
import random
from json import loads

raw_data_path = "/data/xenon/raw/"
SF_MIN=999
SF_MAX=1000
N_PROC=1000
FIRST_RUN=2717
REPROCESS=False
PRESCALE=5
WAVEFORM_PRESCALE=1000
THREADS=1
runsdb = ( "mongodb://"+os.getenv("RUNS_MONGO_USER")+":"
           +os.getenv("RUNS_MONGO_PASSWORD")+"@gw:27017/run")
mondb = ( "mongodb://"+os.getenv("MON_MONGO_USER")+":"
          +os.getenv("MON_MONGO_PASSWORD")+"@gw:27018/admin")


import sys, getopt

def main(argv):

    idnum = None
    try:
        opts, args = getopt.getopt(argv,"n",["num="])
    except getopt.GetoptError:
        print ('monitor.py -n <idnum>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('monitor.py -n <idnum>')
            sys.exit()
        elif opt in ("-n", "--num"):
            idnum = arg

    if idnum == None:
        date = datetime.datetime.utcnow()
        instance_id = datetime.datetime.strftime(date, "%Y%m%d%H%M")
    else:
        instance_id = idnum
    run = True

    # Create options for thread pool
    ops = []
    for i in range(0, THREADS):
        ops.append({
            "id": i,
            "instance_id": instance_id,
            "reprocess": REPROCESS,
            "prescale": PRESCALE
        })


    while run:
        threadPool = ThreadPool(THREADS)
        run = threadPool.map(ScanRunsDB, ops)
        threadPool.close()
        threadPool.join()
        time.sleep(5)
        #run = ScanRunsDB(instance_id, REPROCESS)

    print("Program finished")

def ScanRunsDB(ops):
    '''
    Ask for the most recent run in the runs DB. 
    If it hasn't been procesed, do it. If it has 
    been processed, sleep for 5 seconds and scan again.
    '''
    instance_id = ops['instance_id']
    reprocess = ops['reprocess']
    threadid = ops['id']
    prescale = ops['prescale']

    # Oh no you didn't
    time.sleep(threadid)

    try:
        run_database = pymongo.MongoClient(runsdb)
        most_recent = run_database['run']['runs_new'].find(
            {"detector":"tpc", "number": 
             {"$gt": FIRST_RUN}}).sort("number",1).limit(50)
    except:
        print("Failed connecting to mongodb!")
        return False
    
    if most_recent.count()==0:
        print("Runs DB query came up empty")
        return False

    try: 
        mon_database = pymongo.MongoClient(mondb)['monitor']
    except:
        print("Failed connecting to monitoring DB")
        return False

    # Find most recent run which either (a) wasn't processed or (b) was
    # but with a different instance ID
    savedoc = None
    for doc in most_recent:
        time.sleep(1)
        if doc['name'] in mon_database.collection_names() and not reprocess:
            print("That run has already been processed.")
            continue
        elif reprocess:
            stat = mon_database[doc['name']].find_one({"type": "status"})
            if stat is not None and stat['instance_id'] == instance_id:
                continue
        savedoc = doc
    if savedoc is None:
        print("Didn't find anything to process")
        return True
                
    # Update the status doc to say we've processed this one
    mon_database[savedoc['name']].update_one({'type': 'status'},
                                   {
                                       '$set': {
                                           'instance_id': instance_id,
                                           'type': 'status'
                                       }
                                   }, upsert=True)
    return ProcessRun(savedoc, mondb, prescale)
    
def ProcessRun(most_recent, mondb, prescale):
    '''
    Run pax on the current run. Raw data is split into sub-files
    of 1000 events. It will process events (in order) from each
    sub-file until the next sub-file exists. You can change
    the min and max events per sub-file by setting globals 
    SF_MIN and SF_MAX. 

    Output goes to mondb[collection_name]
    '''

    print("Entering processing for run " + str(most_recent['number']))
    
    collection = most_recent['name']
    current_event = 0
    events_this_file = 0
    current_file_base = ( raw_data_path + collection + "/" + "XENON1T-"+
                          str(most_recent['number'])+"-" )
    
    current_file_event = 0
    run = True
    # I start at the beginning. The first sub-file has to exist
    while run:
        if glob.glob(current_file_base + str(current_file_event).zfill(9) + "*"):

            # Special consideration for last file in run. Need to 
            # skim which event is the last one
            filename = os.path.basename(
                glob.glob(
                    current_file_base + str(current_file_event).zfill(9) + "*")[0])
            if 'temp.' in filename:
                print("Found temporary file, wait for it to finish")
                time.sleep(5)
                continue

            filenamelist = filename.split('-')
            nev = int(filenamelist[4].split('.')[0])
            
            print("Found file " + filename + ", which should contain event " + 
                  str(current_event) + " to " + str(nev-1))
            
            if ProcessEvent(range(current_event, current_event+nev-1, prescale),
                            most_recent['name'], prescale):
                events_this_file +=N_PROC
                current_event +=N_PROC
            
            if events_this_file > SF_MIN:
                # We are allowed to check the next file
                if glob.glob(current_file_base + 
                             str(current_file_event+1000).zfill(9) + "*"):
                    current_file_event += 1000
                    current_event = current_file_event
                    events_this_file = 0
            if events_this_file > SF_MAX:
                # We are not allowed to continue on this file
                current_file_event += 1000
                current_event = current_file_event
                events_this_file = 0
                
        
        else:
            print("Trigger hasn't built file yet. Try sleeping 5 seconds.")
            time.sleep(5)            

        # Check if run is finished. If so try to go until SF_MIN is reached
        if RunFinished(most_recent['number']):
            if events_this_file >= SF_MIN:
                run = False


    return True

def RunFinished(number):
    '''
    Check if a run is finished by looking for the presence of a end_time field
    '''
    try:
        run_database = pymongo.MongoClient(runsdb)
        doc = run_database['run']['runs_new'].find_one({"number":number})
    except:
        print("Failed connecting to mongodb!")
        print("Or couldn't find run " + str(number))
        return False
    if doc is None:
        return False

    if "end" in doc:
        return True
    return False
    

def ProcessEvent(event_numbers, run_name, prescale):
    print("Processing " + str(len(event_numbers)) + " events starting with "+
          str(event_numbers[0])+" from run " + run_name)

    hax.init(experiment="XENON1T", raw_data_local_path=raw_data_path,
         pax_version_policy="loose")

    try:
        mon_database = pymongo.MongoClient(mondb)['monitor']
    except:
        print("Failed connecting to monitoring DB")
        return False


    events = hax.raw_data.process_events(run_name, event_numbers, 
                                         {"pax":
                                          {
                                              'output':'Dummy.DummyOutput',
                                              'encoder_plugin':     None,
                                              'pre_output': [],
                                              'logging_level': 'ERROR'}
                                      }
                                    )
    saved = False
    try:
        for event in events:
            if not saved:
                SaveWaveform(event, run_name)
                saved=True

            # Put it into mongo. Simple stuff for now
            insert_doc = {
                "type": "data",
                "s1": None,
                "s2": None,
                "largest_other_s1": None,
                "largest_other_s2": None,
                "ns1": None,
                "ns2": None,
                "dt": None,
                "x": None,
                "y": None,
                "time": None,
                "interactions": None,
                "prescale": prescale
            }
        
        
            
            insert_doc['ns1'] = len(event.s1s())
            insert_doc['ns2'] = len(event.s2s())
            insert_doc['time'] = event.start_time
            if len(event.s1s())>0:
                insert_doc['s1'] = event.s1s()[0].area
            if len(event.s1s())>1:
                insert_doc['largest_other_s1'] = event.s1s()[1].area
            if len(event.s2s())>0:
                insert_doc['s2'] = event.s2s()[0].area
            if len(event.s2s())>1:
                insert_doc['largest_other_s2'] = event.s2s()[1].area
            if len(event.interactions)>0:
                insert_doc['interactions'] = len(event.interactions)
                insert_doc['dt'] = event.interactions[0].drift_time
                insert_doc['x'] = event.interactions[0].x
                insert_doc['y'] = event.interactions[0].y
            #print(insert_doc)
            mon_database[run_name].insert_one(insert_doc)
    except ValueError as e:
        print("I guess that number isn't in that file. Hax error.")
        print(e)
    return True

def SaveWaveform(event, run_name):

    try:
        wf_database = pymongo.MongoClient(mondb)['waveforms']
    except:
        print("Failed connecting to monitoring DB")
        return False

    # Some events will be too large. Waveforms are mostly zeros. 
    # We can compress them by removing zeros. This keeps the transfer
    # size small. The user's browser will decompress the waveform.
    smaller = CompressEvent(loads(event.to_json()))
    try:
        wf_database[run_name].insert(smaller)
    except Exception as e:
        # Really large waveforms will have to be skipped. This can happen
        # if there are many many channel waveforms.
        print("Error inserting waveform. It's probably too large")

        return

def CompressEvent(event):
    """ Compresses an event by suppressing zeros in waveform in a way the frontend will understand
    Format is the char 'zn' where 'z' is a char and 'n' is the number of following zero bins
    """
    
    for x in range(0, len(event['sum_waveforms'])):

        waveform = event['sum_waveforms'][x]['samples']
        zeros = 0
        ret = []
        
        for i in range(0, len(waveform)):
            if waveform[i] == 0:
                zeros += 1
                continue
            else:
                if zeros != 0:
                    ret.append('z')
                    ret.append(str(zeros))
                    zeros = 0
                ret.append(str(waveform[i]))
        if zeros != 0:
            ret.append('z')
            ret.append(str(zeros))
        event['sum_waveforms'][x]['samples'] = ret

    # Unfortunately we also have to remove the pulses or some events are huge
    del event['pulses']
    return event


if __name__ == "__main__":
   main(sys.argv[1:])
