import subprocess as sp
from sys import executable
from os import path
import yaml

import click
from circus import get_arbiter
from circus.exc import CallError
from circus.util import check_future_exception_and_log
from circus.client import CircusClient
from circus.circusd import daemonize
from aiida import load_dbenv
load_dbenv()
from aiida.settings import get_config, get_default_profile


DEFAULT_PROFILE = get_default_profile('verdi')
VERDI_BIN = path.abspath(path.join(executable, '../verdi'))
VIRTUALENV = path.abspath(path.join(executable, '../../'))
STREAMER_NAME = 'outstream'
FOLDER = path.abspath(path.dirname(__file__))


def get_profile_uuid(profile):
    profile_uuid = get_config()['profiles'][profile]['RMQ_PREFIX']
    return profile_uuid


def get_daemon_name(profile):
    return 'aiida-{}'.format(get_profile_uuid(profile))


def get_daemon_properties(profile):
    daemon = {
        'name': get_daemon_name(profile),
        'cmd': '{} -p {} devel run_daemon'.format(VERDI_BIN, profile),
        'virtualenv': VIRTUALENV,
        'copy_env': True,
        'stdout_stream': {
            'class': 'FileStream',
            'filename': 'test-{}.log'.format(get_profile_uuid(profile)),
        },
        'stderr_stream': {
            'class': 'FileStream',
            'filename': 'test-{}.err.log'.format(get_profile_uuid(profile)),
        },
        'env': {'PYTHONUNBBUFFERED': 'True'}
    }
    return daemon


def streamer_name(msg):
    return '{}-{}'.format(STREAMER_NAME, msg)


def get_streamer(msg):
    streamer = {
        'name': streamer_name(msg),
        'cmd': 'bash outstream.sh',
        'args': msg,
        'stdout_stream': {
            'class': 'FileStream',
            'filename': 'outstream.log'
        },
        'stderr_stream': {
            'class': 'FileStream',
            'filename': 'outstream.log'
        }
    }
    return streamer


class DaemonTamer(object):
    _DAEMON_MAP_FILE = 'daemon_map.yaml'
    _ENDPOINT_TPL = '{host}:{port}'

    def __init__(self, profile):
        self.profile = profile
        self.daemon_name = get_daemon_name(profile)
        self.config = self.get_update_profile()
        self._client = CircusClient()  # endpoint=self.endpoint)

    def generate_new_portnum(self, config=None):
        port = 6000
        used_ports = [profile['port'] for profile in config.values()]
        while port in used_ports:
            port += 2
        return port

    def read_config(self):
        if not path.isfile(self._DAEMON_MAP_FILE):
            return {}
        with open(self._DAEMON_MAP_FILE, 'r') as daemon_map_fo:
            config = yaml.load(daemon_map_fo) or {}
        return config

    def write_config(self, config):
        with open(self._DAEMON_MAP_FILE, 'w') as daemon_map_fo:
            yaml.dump(config, daemon_map_fo)

    def get_update_profile(self):
        config = self.read_config()
        profile_config = config.get(self.profile, {})
        profile_config['uuid'] = get_profile_uuid(self.profile)
        profile_config['host'] = profile_config.get('host', 'tcp://127.0.0.1')
        profile_config['port'] = profile_config.get('port', self.generate_new_portnum(config))
        config[self.profile] = profile_config
        self.write_config(config)
        return config

    @property
    def profile_config(self):
        return self.config[self.profile]

    @property
    def endpoint(self):
        return self._ENDPOINT_TPL.format(host=self.profile_config['host'], port=self.profile_config['port'])

    @property
    def pubsub_endpoint(self):
        return self._ENDPOINT_TPL.format(host=self.profile_config['host'], port=self.profile_config['port'] + 1)

    @property
    def stats_endpoint(self):
        return self._ENDPOINT_TPL.format(host=self.profile_config['host'], port=self.profile_config['port'] + 2)

    @property
    def arbiter_config(self):
        return {
            'logoutput': 'aiida-circus.log',
            'loglevel': 'INFO',
            'debug': False,
            'statsd': True,
            # ~ 'controller': self.endpoint,
            # ~ 'pubsub_endpoint': self.pubsub_endpoint,
            # ~ 'stats_endpoint': self.stats_endpoint
        }

    @property
    def watcher_config(self):
        return get_daemon_properties(self.profile)

    def make_arbiter(self):
        print self.arbiter_config
        print self.watcher_config
        return get_arbiter([self.watcher_config], **self.arbiter_config)

    def pause_daemon(self):
        self._client.stop()

    def unpause_daemon(self):
        self._client.call({
            'command': 'start',
            'properties': {}
        })

    def status(self):
        response = self._client.call({
            'command': 'status',
            'properties': {
                'name': self.daemon_name
            }
        })
        if response.get('status', None):
            return str(response['status'])

    def quit(self):
        try:
            self._client.call({
                'command': 'quit',
                'properties': {}
            })
        except CallError:
            pass


class Client(object):

    def __init__(self, port=6000):
        self._client = CircusClient(endpoint='tcp://127.0.0.1:{}'.format(port))

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

    def start_aiida_daemon(self, profile):
        if not get_daemon_name(profile) in self.list_watchers():
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
                'properties': get_daemon_properties(profile)
            }
            click.echo(command)
            self._client.call(command)
            return self.start_watcher()
        elif not self.is_watcher_active(get_daemon_name(profile)):
            click.echo('starting the watcher...')
            return self.start_watcher(get_daemon_name(profile))
        return None

    def start_outstreamer(self, msg):
        if not streamer_name(msg) in self.list_watchers():
            streamer = get_streamer(msg)
            command = {
                'command': 'add',
                'properties': streamer,
            }
            self._client.call(command)
            return self.start_watcher(streamer_name(msg))
            self._client.call({
                'command': 'set',
                'properties': {
                    'name': streamer_name(msg),
                    'options': {'stdout_stream.filename': 'stream.log'}
                }
            })
        elif not self.is_watcher_active(streamer_name(msg)):
            return self.start_watcher(streamer_name(msg))

    def stop_aiida_daemon(self, profile):
        if get_daemon_name(profile) in self.list_watchers():
            return self._client.call({
                'command': 'stop',
                'properties': {
                    'name': get_daemon_name(profile)
                }
            })
        return None

    def start_watcher(self, name=None):
        response = self._client.call({
            'command': 'start',
            'properties': {
                'name': name
            }
        })

        return bool(response.get('status', None) == 'ok')

    def status(self, name=None):
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


PROFILE_OPT = click.option('--profile', required=True, default=DEFAULT_PROFILE)


@click.group('grinzold')
def grinzold():
    """A proof-of-concept aiida circus daemon."""


@grinzold.group()
def daemon():
    """AiiDA daemon PoC"""


@daemon.command()
@PROFILE_OPT
def start(profile):
    tamer = DaemonTamer(profile)
    daemonize()
    arbiter = tamer.make_arbiter()
    restart = True
    while restart:
        try:
            future = arbiter.start()
            restart = False
            if check_future_exception_and_log(future) is None:
                restart = arbiter._restarting
        except Exception as e:
            # emergency stop
            arbiter.loop.run_sync(arbiter._emergency_stop)
            raise(e)
        except KeyboardInterrupt:
            pass
        finally:
            arbiter = None
    sys.exit(0)


@daemon.command()
@PROFILE_OPT
def pause(profile):
    tamer = DaemonTamer(profile)
    tamer.pause_daemon()


@daemon.command()
@PROFILE_OPT
def unpause(profile):
    tamer = DaemonTamer(profile)
    tamer.unpause_daemon()


@daemon.command()
@PROFILE_OPT
def status(profile):
    tamer = DaemonTamer(profile)
    click.echo(tamer.status())


@daemon.command()
@PROFILE_OPT
def quit(profile):
    tamer = DaemonTamer(profile)
    tamer.quit()


@grinzold.command()
@PROFILE_OPT
def start(profile):
    client = Client()
    client.startup()
    click.echo(client.start_aiida_daemon(profile))
    click.echo(client.status(get_daemon_name(profile)))

@grinzold.command()
@click.argument('msg')
def stream(msg):
    client = Client()
    client.startup()
    click.echo(client.start_outstreamer(msg))
    click.echo(client.status(streamer_name(msg)))

@grinzold.command()
@PROFILE_OPT
def stop(profile):
    client = Client()
    client.stop_aiida_daemon(profile)
    if not client.list_watchers():
        client.quit()

@grinzold.command()
def quit():
    client = Client()
    client.quit()

@grinzold.command()
@PROFILE_OPT
@click.argument('msg', default='bla')
def status(profile, msg):
    client = Client()
    click.echo('aiida-{}: {}'.format(profile, client.status(get_daemon_name(profile))))
    click.echo('streamer: {}'.format(client.status(streamer_name(msg))))


@grinzold.group()
def arbiter():
    """Test using arbiter directly."""


@arbiter.command()
def start():
    daemonize()
    # ~ arbiter = get_arbiter([get_streamer('bla')],
                          # ~ controller='tcp://127.0.0.1:6000',
                          # ~ logoutput='arbiter.log', loglevel='INFO', debug=False, statsd=True)
    arbiter = get_arbiter([get_daemon_properties('ricoh')],
                          controller='tcp://127.0.0.1:6000',
                          logoutput='arbiter.log', loglevel='INFO', debug=False, statsd=True)
    restart = True
    while restart:
        try:
            future = arbiter.start()
            restart = False
            if check_future_exception_and_log(future) is None:
                restart = arbiter._restarting
        except Exception as e:
            # emergency stop
            arbiter.loop.run_sync(arbiter._emergency_stop)
            raise(e)
        except KeyboardInterrupt:
            pass
        finally:
            arbiter = None
    sys.exit(0)

@arbiter.command()
def stop():
    client = Client()
    client.quit()

if __name__ == '__main__':
    grinzold.main()
