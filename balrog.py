import sys
import argparse
import os
try:
    import resource
except ImportError:
    resource = None     # NOQA

import click
from circus import logger, get_arbiter
from circus.circusd import get_maxfd, daemonize, closerange
from circus.arbiter import Arbiter
from circus.pidfile import Pidfile
from circus import __version__
from circus.util import configure_logger
from circus.util import check_future_exception_and_log
from circus.client import CircusClient
from circus.exc import CallError
from aiida import load_dbenv
load_dbenv()
from aiida.common.setup import get_config, get_default_profile, CIRCUS_PORT_KEY, generate_new_circus_port, update_profile


DEFAULT_PROFILE = get_default_profile('verdi')
PROFILE_OPT = click.option('--profile', required=True, default=DEFAULT_PROFILE)
VERDI_BIN = os.path.abspath(os.path.join(sys.executable, '../verdi'))
VIRTUALENV = os.path.abspath(os.path.join(sys.executable, '../../'))


class ProfileConfig(object):
    _ENDPOINT_TPL = 'tcp://127.0.0.1:{port}'

    def __init__(self, profile):
        self.profile = profile
        self.config = get_config()
        self.profile_config = self.config['profiles'][self.profile]
        self.profile_config[CIRCUS_PORT_KEY] = self.profile_config.get(CIRCUS_PORT_KEY, generate_new_circus_port(self.profile))
        update_profile(profile, self.profile_config)

    def get_endpoint(self, port_incr=0):
        port = self.profile_config[CIRCUS_PORT_KEY]
        return self._ENDPOINT_TPL.format(port=port + port_incr)

    def get_client(self):
        return CircusClient(endpoint=self.get_endpoint(), timeout=0.5)

    @property
    def daemon_name(self):
        return 'aiida-{}'.format(self.profile)

    @property
    def cmd_string(self):
        return '{} -p {} devel run_daemon'.format(VERDI_BIN, self.profile)

@click.group('balrog')
def balrog():
    'Manage AiiDA daemons'


@balrog.command('quit')
@PROFILE_OPT
def quit(profile):
    profile_config = ProfileConfig(profile)
    client = profile_config.get_client()
    client.call({'command': 'quit', 'properties': {}})


@balrog.command('status')
@PROFILE_OPT
def status(profile):
    profile_config = ProfileConfig(profile)
    client = profile_config.get_client()
    response = {}
    try:
        response = client.call({
            'command': 'status',
            'properties': {
                'name': profile_config.daemon_name
            }
        })
    except CallError:
        pass

    status = response.get('status', 'shut down.')
    pid = response.get('pid', None)

    click.echo('{}{}'.format(
        str(status),
        str(' pid: {}'.format(pid) if pid else '')
    ))


@balrog.command('start')
@PROFILE_OPT
@click.option('--logger-config', 'loggerconfig',
              help=("The location where a standard Python logger configuration INI, "
                    "JSON or YAML file can be found.  This can be used to override "
                    "the default logging configuration for the arbiter.")
              )
@click.option('--fg', '--foreground', is_flag=True,
              help="Start circusd in the background. Not supported on Windows")
@click.option('--pidfile')
def main(profile, loggerconfig, foreground, pidfile):
    """Run an aiida daemon"""
    import zmq
    try:
        zmq_version = [int(part) for part in zmq.__version__.split('.')[:2]]
        if len(zmq_version) < 2:
            raise ValueError()
    except (AttributeError, ValueError):
        print('Unknown PyZQM version - aborting...')
        sys.exit(0)

    if zmq_version[0] < 13 or (zmq_version[0] == 13 and zmq_version[1] < 1):
        print('circusd needs PyZMQ >= 13.1.0 to run - aborting...')
        sys.exit(0)

    loglevel = 'INFO'
    logoutput = '-'

    if not foreground:
        logoutput = 'balrog-{}.log'.format(profile)
        daemonize()

    # Create the arbiter
    profile_config = ProfileConfig(profile)

    arbiter = get_arbiter(
        controller=profile_config.get_endpoint(0),
        pubsub_endpoint=profile_config.get_endpoint(1),
        stats_endpoint=profile_config.get_endpoint(2),
        logoutput=logoutput,
        loglevel=loglevel,
        debug=False,
        statsd=True,
        pidfile='balrog-{}.pid'.format(profile),  # aiida.common.setup.AIIDA_CONFIG_FOLDER + '/daemon/aiida-{}.pid'.format(uuid)
        watchers=[{
            'name': profile_config.daemon_name,
            'cmd': profile_config.cmd_string,
            'virtualenv': VIRTUALENV,
            'copy_env': True,
            'stdout_stream': {
                'class': 'FileStream',
                'filename': '{}.log'.format(profile_config.daemon_name)
            },
            'env': {
                'PYTHONUNBUFFERED': 'True'
            }
        }]
    )

    # go ahead and set umask early if it is in the config
    if arbiter.umask is not None:
        os.umask(arbiter.umask)

    pidfile = pidfile or arbiter.pidfile or None
    if pidfile:
        pidfile = Pidfile(pidfile)

        try:
            pidfile.create(os.getpid())
        except RuntimeError as e:
            print(str(e))
            sys.exit(1)

    # configure the logger
    loglevel = loglevel or arbiter.loglevel or 'info'
    logoutput = logoutput or arbiter.logoutput or '-'
    loggerconfig = loggerconfig or arbiter.loggerconfig or None
    configure_logger(logger, loglevel, logoutput, loggerconfig)

    # Main loop
    restart = True
    while restart:
        try:
            arbiter = arbiter
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
            if pidfile is not None:
                pidfile.unlink()
    sys.exit(0)


if __name__ == '__main__':
    balrog.main()
