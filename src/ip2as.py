import radix
import pymysql
from contextlib import closing


#Method to pull list of prefixes and corresponding owner AS from DB
def getPrefixASDict(month):
    PrefixASDict={}
    db = pymysql.connect(host="proton.netsec.colostate.edu",
                     user="netsecstudent",
                     passwd="n3ts3cL@bs",
                    db='geoinfo_archive')
    with closing( db.cursor() ) as cur:
        try:
            cur.execute("SELECT OriginAS, BGPPrefix from BGPPrefixGeo where GeoDate like '{0}%';".format(month))
            row=cur.fetchone()
            while row:
                PrefixASDict[row[1]]=row[0]
                row=cur.fetchone()
        except:
            raise Exception('Get Prefix AS Dictionary Failed')

    db.close()
    return PrefixASDict

#Method to pull list of ASes that announce given prefix
def getOriginASes(prefix,month):
    ASList=[]
    db = pymysql.connect(host="proton.netsec.colostate.edu",
                     user="netsecstudent",
                     passwd="n3ts3cL@bs",
                    db='geoinfo_archive')
    with closing( db.cursor() ) as cur:
        try:
            cur.execute("SELECT OriginAS from BGPPrefixGeo where BGPPrefix = '{0}' and GeoDate like '{1}%';".format(prefix,month))
            row=cur.fetchone()
            while row:
                ASList.append(row[0])
                row=cur.fetchone()
        except:
            raise Exception('Get Origin ASes Failed')

    db.close()
    return ASList

def getCAIDAOriginAS(ip):
    ASList=[]
    db = pymysql.connect(host="proton.netsec.colostate.edu",
                     user="netsecstudent",
                     passwd="n3ts3cL@bs",
                    db='caida_itdk')
    with closing( db.cursor() ) as cur:
        try:
            cur.execute("SELECT ASN from IPASMap2014 where IP = '{0}';".format(ip))
            row=cur.fetchone()
            while row:
                ASList.append(row[0])
                row=cur.fetchone()
        except:
            raise Exception('Get Origin ASes Failed')

    db.close()
    return ASList

def getiPlaneOriginAS(ip):
    ASList=[]
    db = pymysql.connect(host="proton.netsec.colostate.edu",
                     user="netsecstudent",
                     passwd="n3ts3cL@bs",
                    db='iplane_db')
    with closing( db.cursor() ) as cur:
        try:
            cur.execute("SELECT ASN from IPASMap2016 where IP = '{0}';".format(ip))
            row=cur.fetchone()
            while row:
                ASList.append(row[0])
                row=cur.fetchone()
        except:
            raise Exception('Get Origin ASes Failed')

    db.close()
    return ASList

def createRadix(geoDate):
    rtree = radix.Radix()
    prefixASDict=getPrefixASDict(geoDate)
    prefixList=prefixASDict.keys()
    for prefix in prefixList:
        rtree.add(prefix)
    return rtree

if __name__ == "__main__":
    geoDate='201601'
    ipsToLookup=['197.239.1.0','129.82.38.1','76.97.98.253','4.79.170.193']

    # Create a new tree
    rtree=createRadix(geoDate)

    for ip in ipsToLookup:
        asFromCAIDA=[]
        asFromiPlane=[]
        asFromLookups=[]
        asMasterSet=set()

        asFromCAIDA=getCAIDAOriginAS(ip)
        for AS in asFromCAIDA:
            asMasterSet.add(AS)

        asFromiPlane=getiPlaneOriginAS(ip)
        for AS in asFromiPlane:
            asMasterSet.add(AS)

        rnode = rtree.search_best(ip)
        lpm=rnode.prefix
        asFromLookups=getOriginASes(lpm,geoDate)
        #for AS in asFromLookups:
        #    print(AS,lpm,ip)
        for AS in asFromLookups:
            asMasterSet.add(AS)

        print(ip,asMasterSet)