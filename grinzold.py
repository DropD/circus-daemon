import subprocess as sp
from sys import executable
from os import path

import click
from circus.client import CircusClient
from aiida import load_dbenv
load_dbenv()
from aiida.settings import profile_conf


PROFILE_UUID = profile_conf['RMQ_PREFIX']
WATCHER_NAME = 'aiida-{}'.format(PROFILE_UUID)


class Client(object):

    def __init__(self):
        self._client = CircusClient()

    def startup(self):
        click.echo('startup')
        sp.call(['circusd', 'basic.ini', '--daemon'])

    def quit(self):
        self._client.call({
            'command': 'quit',
            'properties': {}
        })

    def stop(self):
        self._client.stop()

    def start_aiida_daemon(self):
        if not WATCHER_NAME in self.list_watchers():
            # ~ command = {
                # ~ 'command': 'add',
                # ~ 'properties': {
                    # ~ 'name': WATCHER_NAME,
                    # ~ 'cmd': 'verdi devel run_daemon',
                    # ~ 'virtualenv': path.abspath(path.join(executable, '../../')),
                    # ~ 'copy_env': True,
                    # ~ 'pidfile': '{}/circus-aiida-{}'.format(path.dirname(path.abspath(__file__)), PROFILE_UUID),
                    # ~ 'autostart': True,
                    # ~ 'warmup_delay': 5
                # ~ }
            # ~ }
            click.echo('adding the watcher...')
            command = {
                'command': 'add',
                'properties': {
                    'name': WATCHER_NAME,
                    # ~ 'cmd': 'verdi devel run_daemon',
                    'cmd': '/home/hauselmann/miniconda2/envs/aiida-dev-workflows/bin/verdi devel run_daemon',
                    'virtualenv': '/home/hauselmann/miniconda2/envs/aiida-dev-workflows',
                    'copy_env': True,
                    # ~ 'pidfile': '/home/hauselmann/Documents/circus-daemon/aiida-ipy.pid',
                    # ~ 'stdout_stream.class': 'FileStream',
                    # ~ 'stdout_stream.filename': '/home/hauselmann/Documents/circus-daemon/aiida-ipy.log',
                    # ~ 'loglevel': 'debug',
                    # ~ 'logoutput': '/home/hauselmann/Documents/circus-daemon/aiida-ipy-watcher.log',
                    'autostart': True,
                    # ~ 'warmup_delay': 5
                }
            }
            click.echo(command)
            self._client.call(command)
            return self.start_watcher()
        elif not self.is_daemon_active():
            click.echo('starting the watcher...')
            return self.start_watcher()
        return None

    def stop_aiida_daemon(self):
        if WATCHER_NAME in self.list_watchers():
            return self._client.call({
                'command': 'stop',
                'properties': {
                    'name': WATCHER_NAME
                }
            })
        return None

    def start_watcher(self):
        response = self._client.call({
            'command': 'start',
            'properties': {
                'name': WATCHER_NAME
            }
        })

        return bool(response.get('status', None) == 'ok')

    def status(self):
        response = self._client.call({
            'command': 'status',
            'properties': {
                'name': WATCHER_NAME
            }
        })
        if response.get('status', None):
            return str(response['status'])

    def is_daemon_active(self):
        return bool(self.status() == 'active')

    def list_watchers(self):
        response = self._client.call({
            'command': 'list',
            'properties': {}
        })
        return [str(watcher) for watcher in response['watchers']] if response['status'] == u'ok' else []


@click.group('grinzold')
def grinzold():
    """A proof-of-concept aiida circus daemon."""


@grinzold.command()
def start():
    client = Client()
    client.startup()
    click.echo(client.start_aiida_daemon())
    click.echo(client.status())

@grinzold.command()
def stop():
    client = Client()
    client.stop_aiida_daemon()
    if not client.list_watchers():
        client.quit()

@grinzold.command()
def quit():
    client = Client()
    client.quit()

@grinzold.command()
def status():
    client = Client()
    click.echo(client.status())


if __name__ == '__main__':
    grinzold.main()
