import json
import urllib2
import numpy as np
from pyhive import hive
from time import time


class HiveInteraction(object):
    def __int__(self):
        self.cursor = None

    def connect_to_hive(self, ip='localhost'):
        self.cursor = hive.connect(host=ip, port='10000').cursor()
        print "Connection to hive is established!"

    def create_data_segment(self, hive_table, day_ts):
        qry_rawdata = """
        create table smp as
        select case when device.rawidfa is not null then device.rawidfa
        when device.md5hashedidfa is not null then device.md5hashedidfa
        when device.sha1hashedidfa is not null then device.sha1hashedidfa end as idfa,
        device.os,
        app.bundle,
        app.appcategories
        from {}
        where day_ts = {}
        and app.bundle is not null
        """
        self.cursor.execute("drop table if exists smp")
        self.cursor.execute(qry_rawdata.format(hive_table, day_ts))
        self.cursor.execute("select count(*), count(distinct bundle), count(distinct idfa) from smp")
        print "auctions, apps, idfas: \n{}".format(self.cursor.fetchall())

    def extract_ios_apps(self):
        qry_apps = "select distinct bundle from smp where os = 'iOS'"
        self.cursor.execute(qry_apps)
        ios_apps = self.cursor.fetchall()
        valid_ios_apps = [app[0] for app in ios_apps if app[0].isdigit() and len(app[0]) in (9, 10)]
        return valid_ios_apps


def batch(iterable, n=200):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


def extract_categories(app):
    try:
        return app["trackId"], app["genres"], app["price"]
    except KeyError:
        pass


def query_itunes(apps, i, num_iterations):
    print "Iteration # {} out of {}".format(i, num_iterations)
    t0 = time()
    apps_str = ','.join(apps)
    response = urllib2.urlopen("https://itunes.apple.com/lookup?id={}".format(apps_str))
    itunes_data = json.load(response)["results"]
    itunes_categories = map(extract_categories, itunes_data)
    print "Response time is {} sec.".format(time() - t0)
    return itunes_categories


def main():
    """
    hive1 = HiveInteraction()
    hive1.connect_to_hive("ec2-34-201-71-157.compute-1.amazonaws.com")
    hive1.create_data_segment("sampling.rawdata_sampling_orc", "20170924")
    ios_apps = hive1.extract_ios_apps()
    """
    with open("/mnt/spotad/inprocess/ios") as f:
        ios_apps = f.readlines()
    len(ios_apps)   # 13953

    ios_apps = [app.replace("\n", "") for app in ios_apps]
    ios_apps = [app for app in ios_apps if app.isdigit() and len(app) in (9, 10)]
    len(ios_apps)  # 13458

    ios_app_categories = []
    i = 0
    num_iterations = len(ios_apps)/200 + 1
    for x in batch(ios_apps):
        i += 1
        ios_app_categories.extend(query_itunes(x, i, num_iterations))

    len(ios_app_categories)  # 11669
    ios_app_categories = [app for app in ios_app_categories if app is not None]
    len(ios_app_categories)  # 11665

    outlist = ['\t'.join([str(tpl[0]), str(tpl[1]), str(tpl[2])]) + '\n' for tpl in ios_app_categories]

    with open("/mnt/spotad/inprocess/ios_app_store_info", "w") as f:
        f.writelines(outlist)

