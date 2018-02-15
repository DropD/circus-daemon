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
VERDI_BIN = path.abspath(path.join(executable, '../verdi'))
VIRTUALENV = path.abspath(path.join(executable, '../../'))
STREAMER_NAME = 'outstream'
FOLDER = path.abspath(path.dirname(__file__))


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
                    'cmd': '{} devel run_daemon'.format(VERDI_BIN),
                    'virtualenv': VIRTUALENV,
                    'copy_env': True,
                    # ~ 'pidfile': '',
                    'stdout_stream.class': 'FileStream',
                    'stdout_stream.filename': 'test.log',
                    # ~ 'loglevel': 'debug',
                    # ~ 'logoutput': '',
                    'autostart': True,
                    # ~ 'warmup_delay': 5
                }
            }
            click.echo(command)
            self._client.call(command)
            return self.start_watcher()
        elif not self.is_watcher_active():
            click.echo('starting the watcher...')
            return self.start_watcher()
        return None

    def start_outstreamer(self):
        if not STREAMER_NAME in self.list_watchers():
            command = {
                'command': 'add',
                'properties': {
                    'name': STREAMER_NAME,
                    'cmd': 'bash outstream.sh',
                    'stdout_stream.class': 'logging.handlers.RotatingFileHandler',
                    'stdout_stream.filename': 'stream.log',
                }
            }
            self._client.call(command)
            return self.start_watcher(STREAMER_NAME)
            self._client.call({
                'command': 'set',
                'properties': {
                    'name': STREAMER_NAME,
                    'options': {'stdout_stream.filename': 'stream.log'}
                }
            })
        elif not self.is_watcher_active(STREAMER_NAME):
            return self.start_watcher(STREAMER_NAME)

    def stop_aiida_daemon(self):
        if WATCHER_NAME in self.list_watchers():
            return self._client.call({
                'command': 'stop',
                'properties': {
                    'name': WATCHER_NAME
                }
            })
        return None

    def start_watcher(self, name=WATCHER_NAME):
        response = self._client.call({
            'command': 'start',
            'properties': {
                'name': name
            }
        })

        return bool(response.get('status', None) == 'ok')

    def status(self, name=WATCHER_NAME):
        response = self._client.call({
            'command': 'status',
            'properties': {
                'name': name
            }
        })
        if response.get('status', None):
            return str(response['status'])

    def is_watcher_active(self, name):
        return bool(self.status(name) == 'active')

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
def stream():
    client = Client()
    client.startup()
    click.echo(client.start_outstreamer())
    click.echo(client.status(STREAMER_NAME))

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
    click.echo('aiida: {}'.format(client.status()))
    click.echo('streamer: {}'.format(client.status(STREAMER_NAME)))


if __name__ == '__main__':
    grinzold.main()
