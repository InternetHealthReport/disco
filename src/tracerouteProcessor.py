from ripe.atlas.cousteau import AtlasStream,AtlasResultsRequest
from datetime import datetime

class tracerouteProcessor():

    def __init__(self,burstEventInfo,useStream=False):
        self.burstEventInfo=burstEventInfo
        self.USE_STREAM=useStream

    def onTracerouteResponse(self,*args):
        item=args[0]
        print(item)

    def pullTraceroutes(self):
        msmIDs=[]
        #files=['data/anchorMsmIdsv4.txt','data/builtinMsmIdsv4.txt']
        files=['data/builtinMsmIdsv4.txt']
        for file in files:
            with open(file,'r') as fp:
                for line in fp:
                    l=int(line.rstrip('\n').split(':')[1])
                    msmIDs.append(l)
        if self.USE_STREAM:
            try:

                #Read Stream
                atlas_stream = AtlasStream()
                atlas_stream.connect()

                # Probe's connection status results
                channel = "result"

                atlas_stream.bind_channel(channel, onTracerouteResponse)
                startTime=1461911417.0
                endTime=1461969358.5

                for msm in msmIDs:
                    #print(msm)
                    stream_parameters = {"msm": msm,"startTime":startTime,"endTime":endTime}
                    atlas_stream.start_stream(stream_type="result", **stream_parameters)

                atlas_stream.timeout()

                # Shut down everything
                atlas_stream.disconnect()
            except:
                print('Unexpected Event. Quiting.')
                atlas_stream.disconnect()
        else:
            startTime=datetime.fromtimestamp(1461911417.0)
            endTime=datetime.fromtimestamp(1461969358.5)
            #for msm in msmIDs:
            kwargs = {
                "msm_id": msmIDs,
                "start":startTime,
                "endTime":endTime,
                #"probe_ids": [1,2,3,4]
            }

            is_success, results = AtlasResultsRequest(**kwargs).create()

            if is_success:
                print(results)
