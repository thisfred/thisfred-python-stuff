#!/usr/bin/env python
from os.path import isdir
from os import listdir, system

local = '/home/eric/ogg'
iaudio = '/media/IAUDIO/MUSIC'

localdirs = listdir(local)
iaudiodirs = listdir(iaudio)

for entry in iaudiodirs:
    iaudio_path = iaudio + '/' + entry
    local_path = local + '/' + entry
    if isdir(iaudio_path):
        if entry not in localdirs:
            print "synching %s from iaudo to local" % iaudio_path
            system('rsync --size-only --delete --delete-excluded --exclude-from=/home/eric/.rsync/exclude -avz --no-group %s/ %s' % (iaudio_path, local_path))
        else:
            print "synching %s from local to iaudo" % entry
            system('rsync --size-only --delete --delete-excluded --exclude-from=/home/eric/.rsync/exclude -avz --no-group %s/ %s' % (local_path, iaudio_path))
        
