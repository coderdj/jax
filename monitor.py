import pymongo
import os
import glob
import time
import hax

raw_data_path = "/data/xenon/raw/"
SF_MIN=100
SF_MAX=1000
N_PROC=50
runsdb = ( "mongodb://"+os.getenv("RUNS_MONGO_USER")+":"
           +os.getenv("RUNS_MONGO_PASSWORD")+"@gw:27017/run")
mondb = ( "mongodb://"+os.getenv("MON_MONGO_USER")+":"
          +os.getenv("MON_MONGO_PASSWORD")+"@gw:27018/admin")


def main():
    
    run = True

    while run:
        run = ScanRunsDB()

    print("Program finished")

def ScanRunsDB():
    '''
    Ask for the most recent run in the runs DB. 
    If it hasn't been procesed, do it. If it has 
    been processed, sleep for 5 seconds and scan again.
    '''

    try:
        run_database = pymongo.MongoClient(runsdb)
        most_recent = run_database['run']['runs_new'].find(
            {"detector":"tpc"}).sort("number",-1)[0]
    except:
        print("Failed connecting to mongodb!")
        return False
    
    if most_recent is None:
        print("Runs DB query came up empty")
        return False

    try: 
        mon_database = pymongo.MongoClient(mondb)['monitor']
    except:
        print("Failed connecting to monitoring DB")
        return False

    if most_recent['name'] in mon_database.collection_names():
        print("That run has already been processed. Sleeping for five seconds")
        time.sleep(5)
        return True
        
    return ProcessRun(most_recent, mondb)
    
def ProcessRun(most_recent, mondb):
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
    # I start at the beginning. The first sub-file hast to exist
    while run:
        if glob.glob(current_file_base + str(current_file_event).zfill(9) + "*"):
            if ProcessEvent(range(current_event, current_event+N_PROC),
                            most_recent['name']):
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
    

def ProcessEvent(event_numbers, run_name):
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
                                              'logging_level': 'ERROR'}
                                      }
                                    )

    for event in events:

        # Put it into mongo. Simple stuff for now
        insert_doc = {
            "s1": None,
            "s2": None,
            "largest_other_s1": None,
            "largest_other_s2": None,
            "ns1": None,
            "ns2": None,
            "dt": None,
            "x": None,
            "y": None
        }
        
        
        
        insert_doc['ns1'] = len(event.s1s())
        insert_doc['ns2'] = len(event.s2s())
        if len(event.s1s())>0:
            insert_doc['s1'] = event.s1s()[0].area
        if len(event.s1s())>1:
            insert_doc['largest_other_s1'] = event.s1s()[1].area
        if len(event.s2s())>0:
            insert_doc['s2'] = event.s2s()[0].area
        if len(event.s2s())>1:
            insert_doc['largest_other_s2'] = event.s2s()[1].area
        if len(event.interactions)>0:
            insert_doc['dt'] = event.interactions[0].drift_time
            insert_doc['x'] = event.interactions[0].x
            insert_doc['y'] = event.interactions[0].y
        print(insert_doc)
        mon_database[run_name].insert_one(insert_doc)
    return True

main()
