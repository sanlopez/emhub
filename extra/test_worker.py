#!/usr/bin/env python
# **************************************************************************
# *
# * Authors:     J.M. de la Rosa Trevin (delarosatrevin@gmail.com)
# *
# * This program is free software; you can redistribute it and/or modify
# * it under the terms of the GNU General Public License as published by
# * the Free Software Foundation; either version 3 of the License, or
# * (at your option) any later version.
# *
# * This program is distributed in the hope that it will be useful,
# * but WITHOUT ANY WARRANTY; without even the implied warranty of
# * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# * GNU General Public License for more details.
# *
# **************************************************************************
import json
import os
import sys
import time
import logging
import argparse
import threading
from datetime import datetime, timedelta, timezone
from glob import glob
from collections import OrderedDict
import configparser
from pprint import pprint
import traceback

from emtools.utils import Pretty, Process, Path, Color, System
from emtools.metadata import EPU, MovieFiles, StarFile

from emhub.client import config
from emhub.client.worker import (TaskHandler, DefaultTaskHandler, CmdTaskHandler,
                                 Worker)
from emhub.client.session_worker import SessionTaskHandler, SessionWorker

class TestSessionTaskHandler(TaskHandler):
    def __init__(self, *args, **kwargs):
        TaskHandler.__init__(self, *args, **kwargs)
        targs = self.task['args']
        self.session_id = int(targs['session_id'])
        self.action = targs.get('action', 'Empty-Action')
        self.info(f"Retrieving session {self.session_id} from EMhub "
                  f"({config.EMHUB_SERVER_URL})")
        self.session = self.dc.get_session(self.session_id)

    def update_session_extra(self, extra):
        def _update_extra():
            extra['updated'] = Pretty.now()
            self.worker.request('update_session_extra',
                         {'id': self.session['id'], 'extra': extra})
            return True

        return self._request(_update_extra, 'updating session extra')


    def process(self):
        try:
            if self.action == 'monitor':
                return self.monitor()
            elif self.action == 'otf':
                return self.otf()
            elif self.action == 'copy_to_irods':
                return self.copy_to_irods()
            raise Exception(f"Unknown action {self.action}")
        except Exception as e:
            self.update_task({'error': str(e), 'done': 1})
            self.stop()

    def monitor(self):
        # update raw path
        raw_path = f"{os.path.join(self.session['acquisition']['raw_path'], self.session['name'])}"
        acq = self.session['acquisition']
        repeat_until_date = datetime.fromisoformat(self.session['start']) + timedelta(days=3)


        print(Color.bold(f"session_id = {self.session['id']}, monitoring files..."))
        print(f"    path: {raw_path}")

        if self.count == 1:
            self.mf = MovieFiles()
            self.mf._moviesSuffix.append(acq['images_pattern'])

        self.mf.scan(raw_path)
        update_args = self.mf.info()
        # get updated session (to avoid inconsistencies with other task)
        self.session = self.dc.get_session(self.session['id'])
        raw = self.session['extra']['raw']
        raw.update(update_args)
        raw.update({'path': raw_path})
        self.update_session_extra({'raw': raw})

        if datetime.now(timezone.utc) > repeat_until_date:
            self.stop()
            update_args['done'] = 1

        # Remove dict from the task update
        if 'files' in update_args:
            del update_args['files']
        self.update_task(update_args)


    def otf(self):
        # update raw path
        self.session['extra']['raw']['path'] = f"{os.path.join(self.session['acquisition']['raw_path'], self.session['name'])}"
        extra = self.session['extra']
        raw = extra['raw']
        raw_path = raw['path']
        otf = extra['otf']
        acq = self.session['acquisition']

        otf['status'] = 'created'

        if extra['otf']['otf_workflow'] == 'Scipion':
            scipion_workflow_template_path = extra['otf']['scipion_workflow']
            scipion_config = self.request_config('scipion')
            scipion_path = scipion_config['scipion_path']
            scipion_projects_path = scipion_config['scipion_user_data_path']

            self.pl.system(f"{scipion_path} template {scipion_workflow_template_path} project_name={self.session['name']} \
                            moviesPath={raw_path} \
                            filesPattern={acq['images_pattern']} \
                            voltage={acq['voltage']} \
                            spherical={acq['cs']} \
                            magnification={acq['magnification']} \
                            sampling={acq['pixel_size']} \
                            dosePerFrame={acq['dose']} \
                            gainPath={raw_path}/*.gain \
                            &")

            otf['path'] = f"{scipion_projects_path}/{self.session['name']}"

            self.update_task({'otf_path': f"{scipion_projects_path}/{self.session['name']}",
                              'otf_status': otf['status'],
                              'count': self.count,
                              'done': 1})

            #self.update_session_extra({'raw': raw})
            self.update_session_extra({'otf': otf})

            self.stop()


    def copy_to_irods(self):
        print("Trying to copy to iRODS...")
        self.session = self.dc.get_session(self.session['id'])
        extra = self.session['extra']
        raw = extra['raw']
        otf = extra['otf']
        raw_path = raw.get('path', None)
        otf_path = otf.get('path', None)

        from copy_data import iRODSManager
        im = iRODSManager()
        irods_config = self.request_config('irods')
        im.irods_zone = irods_config['irods_zone']
        im.irods_host = irods_config['irods_host']
        im.irods_user = irods_config['irods_user']
        im.irods_port = irods_config['irods_port']
        im.irods_pass = irods_config['irods_pass']
        im.irods_parent_collection = irods_config['irods_parent_collection']

        if raw_path:
            print(f"Trying to copy raw data ({raw_path}) to iRODS...")
            create_ticket = False if (raw.get('irods', {}).get('linux', '') and raw.get('irods', {}).get('windows', '')) else True
            print("...and WILL create ticket!") if create_ticket else print("...and WILL NOT create ticket!")
            success, info = im.copy_data(f"{self.session['name']}_raw", raw_path, create_ticket=create_ticket)
            if success and create_ticket:
                # get updated session (to avoid inconsistencies with other task)
                self.session = self.dc.get_session(self.session['id'])
                raw = self.session['extra']['raw']
                raw.setdefault('irods', {})['linux'] = info['irods_retrieval_script_linux']
                raw.setdefault('irods', {})['windows'] = info['irods_retrieval_script_windows']
                self.update_session_extra({'raw': raw})
        if otf_path:
            print(f"Trying to copy OTF data ({otf_path}) to iRODS...")
            create_ticket = False if (otf.get('irods', {}).get('linux', '') and otf.get('irods', {}).get('windows', '')) else True
            print("...and WILL create ticket!") if create_ticket else print("...and WILL NOT create ticket!")
            success, info = im.copy_data(f"{self.session['name']}_otf", otf_path, create_ticket=create_ticket)
            if success and create_ticket:
                # get updated session (to avoid inconsistencies with other task)
                self.session = self.dc.get_session(self.session['id'])
                otf = self.session['extra']['otf']
                otf.setdefault('irods', {})['linux'] = info['irods_retrieval_script_linux']
                otf.setdefault('irods', {})['windows'] = info['irods_retrieval_script_windows']
                self.update_session_extra({'otf': otf})


class TestSessionWorker(Worker):
    def handle_tasks(self, tasks):
        for t in tasks:
            if t['name'] == 'command':
                handler = CmdTaskHandler(self, t)
            elif t['name'] == 'session':
                handler = TestSessionTaskHandler(self, t)
            else:
                handler = DefaultTaskHandler(self, t)
            handler.start()


if __name__ == '__main__':
    worker = TestSessionWorker(debug=True)
    worker.run()









