# encoding=utf8
from distutils.version import StrictVersion
import datetime
import hashlib
import os
import shutil
import socket
import subprocess
import sys
import time
import re
from subprocess import call

sys.path.insert(0, os.getcwd())

try:
    import requests
except ImportError:
    print('Please install or update the requests module.')
    sys.exit(1)
try:
    from internetarchive import upload
except ImportError:
    print('Please install or update the requests module.')
    sys.exit(1)

import seesaw
from seesaw.config import realize, NumberConfigValue
from seesaw.externalprocess import WgetDownload, ExternalProcess
from seesaw.item import ItemInterpolation, ItemValue
from seesaw.pipeline import Pipeline
from seesaw.project import Project
from seesaw.task import SimpleTask, SetItemKey, LimitConcurrent
from seesaw.tracker import PrepareStatsForTracker, GetItemFromTracker, \
    UploadWithTracker, SendDoneToTracker
from seesaw.util import find_executable

# check the seesaw version
if StrictVersion(seesaw.__version__) < StrictVersion("0.8.5"):
    raise Exception("This pipeline needs seesaw version 0.8.5 or higher.")

###########################################################################
# Find a useful Wpull executable.
#
# WPULL_EXE will be set to the first path that
# 1. does not crash with --version, and
# 2. prints the required version string
PYTHON35_EXE = find_executable(
    "Python 3.5",
    re.compile(r"^Python 3\.5"),
    [
        "/usr/local/bin/python3.5",
        "python3.5",
        "python3",
        "python",
    ]
)

if not PYTHON35_EXE:
    raise Exception("No usable python3.5 library found.")
if not os.environ.get('s3access'):
    raise Exception("s3 access key missing")
if not os.environ.get('s3secret'):
    raise Exception("s3 secret key missing")

###########################################################################
# The version number of this pipeline definition.
#
# Update this each time you make a non-cosmetic change.
# It will be added to the WARC files and reported to the tracker.
VERSION = "20190713.01"
TRACKER_ID = 'ng-dedupetst'
TRACKER_HOST = 'tracker.kiska.pw'


###########################################################################
# This section defines project-specific tasks.
#
# Simple tasks (tasks that do not need any concurrency) are based on the
# SimpleTask class and have a process(item) method that is called for
# each item.
class CheckIP(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, "CheckIP")
        self._counter = 0

    def process(self, item):
        # NEW for 2014! Check if we are behind firewall/proxy

        if self._counter <= 0:
            item.log_output('Checking IP address.')
            ip_set = set()

            ip_set.add(socket.gethostbyname('twitter.com'))
            ip_set.add(socket.gethostbyname('facebook.com'))
            ip_set.add(socket.gethostbyname('youtube.com'))
            ip_set.add(socket.gethostbyname('microsoft.com'))
            ip_set.add(socket.gethostbyname('icanhas.cheezburger.com'))
            ip_set.add(socket.gethostbyname('archiveteam.org'))

            if len(ip_set) != 6:
                item.log_output('Got IP addresses: {0}'.format(ip_set))
                item.log_output(
                    'Are you behind a firewall/proxy? That is a big no-no!')
                raise Exception(
                    'Are you behind a firewall/proxy? That is a big no-no!')

        # Check only occasionally
        if self._counter <= 0:
            self._counter = 10
        else:
            self._counter -= 1

class PrepareDirectories(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, "PrepareDirectories")

    def process(self, item):
        dirname = "/".join((item['data_dir'], item['item_name']))
        if os.path.isdir(dirname):
            shutil.rmtree(dirname)
        os.makedirs(dirname)
        completeddir = "data/completed"
        if not os.path.isdir(completeddir):
            print("making directories")
            os.makedirs(completeddir)
        item['item_dir'] = dirname

class UploadToIAArgs(object):
    def realize(self, item):
        access_key = os.environ.get('s3access')
        secret_key = os.environ.get('s3secret')
        iaauth = ":".join((access_key,secret_key ))
        file_name = "data/completed/%(item_name)s.warc.gz" % item
        itemheader = "x-archive-meta-title:Archive Team Newsgrab: %(item_name)s" % item
        destination = "https://s3.us.archive.org/archiveteam_%(item_name)s/%(item_name)s.megawarc.warc.gz" % item
        sizehint = os.path.getsize(file_name)
        UploadToIAArgs_args = [
            "curl", "-v", "--location", "--fail",
            "--speed-limit", "1", "--speed-time", "900", \
            "--header", "x-archive-queue-derive:1", \
            "--header", "x-archive-keep-old-version:0", \
            "--header", "x-archive-meta-collection:archiveteam_newssites", \
            "--header", "x-archive-meta-mediatype:web", \
            "--header", itemheader, \
            "--header", "x-archive-meta-language:eng", \
            "--header", "x-archive-size-hint:" + str(sizehint), \
            "--header", "authorization: LOW " + iaauth, \
            "--upload-file", file_name, \
            destination
        ]
        return realize(UploadToIAArgs_args, item)

class UploadToIA(ExternalProcess):
    '''Use curl to upload to IA.'''
    def __init__(self, args):
        print(args)
        ExternalProcess.__init__(self, "UploadToIA",
            args=args,)

class MoveFiles(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, "MoveFiles")

    def process(self, item):
        os.rename("%(data_dir)s/%(item_name)s.deduplicated.warc.gz" % item,
              "data/completed/%(item_name)s.warc.gz" % item)

        shutil.rmtree("%(item_dir)s" % item)

class DedupeArgs(object):
    def realize(self, item):
        dedupe_args = [
            PYTHON35_EXE,
            "-u",
            "deduplicate.py",
            ItemInterpolation("%(data_dir)s/%(item_name)s.warc.gz"),
        ]
        return realize(dedupe_args, item)

class WgetArgs(object):
    def realize(self, item):
        item_name = item['item_name']
        warcfile = item['data_dir'] + "/" + item['item_name'] + ".warc.gz"
        wget_args = [
            'wget',
            '-nv',
            '-U', 'ArchiveTeam; Googlebot/2.1',
            '--tries', '5',
            '--waitretry', '5',
            '-O', warcfile,
	    ItemInterpolation("https://archive.org/download/archiveteam_%(item_name)s/%(item_name)s.megawarc.warc.gz")
        ]
        return realize(wget_args, item)

class DeduplicateWarcExtProc(ExternalProcess):
    '''Deduplicate warc and capture exceptions.'''
    def __init__(self, args):
        ExternalProcess.__init__(self, "DeduplicateWarcExtProc",
            args=args,)

class WgetDownload(ExternalProcess):
    '''Download warc and capture exceptions.'''
    def __init__(self, args):
        ExternalProcess.__init__(self, "WgetDownload",
            args=args,)

def get_hash(filename):
    with open(filename, 'rb') as in_file:
        return hashlib.sha256(in_file.read()).hexdigest()

CWD = os.getcwd()
PIPELINE_SHA256 = get_hash(os.path.join(CWD, 'pipeline.py'))
WARRIOR_INSTALL_SHA256 = get_hash(os.path.join(CWD, 'warrior-install.sh'))

def stats_id_function(item):
    d = {
        'pipeline_hash': PIPELINE_SHA256,
        'warrior_install_hash': WARRIOR_INSTALL_SHA256,
        'python_version': sys.version,
    }

    return d

###########################################################################
# Initialize the project.
#
# This will be shown in the warrior management panel. The logo should not
# be too big. The deadline is optional.
project = Project(
    title="Newsgrabber-Deduplication",
    project_html="""
        <img class="project-logo" alt="Project logo" src="http://archiveteam.org/images/thumb/f/f3/Archive_team.png/235px-Archive_team.png" height="50px" title=""/>
        <h2>archiveteam.org <span class="links"><a href="http://archiveteam.org/">Website</a> &middot; <a href="http://tracker.archiveteam.org/newsgrabber/">Leaderboard</a></span></h2>
        <p>We did a bad - Recovering from it....</p>
    """
)

pipeline = Pipeline(
    CheckIP(),
    GetItemFromTracker("http://%s/%s" % (TRACKER_HOST, TRACKER_ID), downloader,
        VERSION),
    PrepareDirectories(),
    WgetDownload(
        WgetArgs(),
    ),
    DeduplicateWarcExtProc(
        DedupeArgs()
    ),
    PrepareStatsForTracker(
        defaults={"downloader": downloader, "version": VERSION},
        file_groups={
            "data": [
                 ItemInterpolation("%(data_dir)s/%(item_name)s.deduplicated.warc.gz")
            ]
        },
        id_function=stats_id_function,
    ),
    MoveFiles(),
    LimitConcurrent(
        NumberConfigValue(min=1, max=4, default="1",
            name="shared:rsync_threads", title="Rsync threads",
            description="The maximum number of concurrent uploads."),
        UploadToIA(
            UploadToIAArgs()
        ),
    ),
    SendDoneToTracker(
        tracker_url="http://%s/%s" % (TRACKER_HOST, TRACKER_ID),
        stats=ItemValue("stats")
    )
)
