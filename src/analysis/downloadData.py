import sys
import os 
from datetime import datetime
from datetime import timedelta
from dateutil import relativedelta
from ripe.atlas.cousteau import AtlasResultsRequest 
import json
import gzip


def downloadData(start, end, msmId=7000, timeWindow = timedelta(minutes=24*60) ):
    errors = []

    # Get measurments results
    currDate = start
    while currDate+timeWindow<end:
        path = "data/%s/%s" % (currDate.year, currDate.month)
        try:
            print("%s:  measurement id %s" % (currDate, msmId) )
            if not os.path.exists(path):
                os.makedirs(path)
            if os.path.exists("%s/%s_msmId%s.json.gz" % (path, currDate, msmId)):
                continue

            kwargs = {
                "msm_id": msmId,
                "start": currDate,
                "stop": currDate+timeWindow,
            }

            is_success, results = AtlasResultsRequest(**kwargs).create()

            if is_success :
                # Output file
                fi = gzip.open("%s/%s_msmId%s.json.gz" % (path, currDate, msmId) ,"wb")
                print("Storing data for %s measurement id %s" % (currDate, msmId) )
                json.dump(results, fi)
                fi.close()

            else:
                errors.append("%s: msmId=%s" % (currDate, msmId))
                print "error: %s: msmId=%s" % (currDate, msmId)

        except ValueError:
            errors.append("%s: msmId=%s" % (currDate, msmId))
            print "error: %s: msmId=%s" % (currDate, msmId)

        finally:
            currDate += timeWindow

    if errors:
        print("Errors with the following parameters:")
        print(errors)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print "usage: %s year month" % sys.argv[0]
        sys.exit()

    start = datetime(int(sys.argv[1]), int(sys.argv[2]), 1)
    end= start + relativedelta.relativedelta(months=1)

    downloadData(start, end) 
