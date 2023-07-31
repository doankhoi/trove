from oslo_log import log as logging
from trove.guestagent.datastore import service
from trove.guestagent.utils import docker as docker_util
from trove.instance import service_status
from trove.guestagent.common import configuration
from trove.common import cfg
from trove.common import stream_codecs
from trove.guestagent.common import operating_system
from trove.common import exception
from trove.common import utils


SUPER_USER_NAME = "redis"
CONFIG_FILE = "/etc/redis/redis.conf"
REDIS_PID_FILE = "/etc/run/redis/redis-server.pid"
REDIS_PORT = '6379'
CNF_EXT = 'conf'


# The same with include_dir config option
CNF_INCLUDE_DIR = '/etc/redis/conf.d'

# The same with the path in archive_command config option.
WAL_ARCHIVE_DIR = '/var/lib/redis/data/wal_archive'

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class RedisAppStatus(service.BaseDbStatus):
    def __init__(self, docker_client):
        super(RedisAppStatus, self).__init__(docker_client)

    def get_actual_db_status(self):
        """Check database service status."""
        status = docker_util.get_container_status(self.docker_client)
        if status == "running":
            config_options = operating_system.read_file(CONFIG_FILE)
            root_pass = config_options.get('requirepass')
            cmd = "redis-cli -a %s PING" % root_pass
            try:
                docker_util.run_command(self.docker_client, cmd)
                return service_status.ServiceStatuses.HEALTHY
            except Exception as exc:
                LOG.warning('Failed to run docker command, error: %s',
                            str(exc))
                container_log = docker_util.get_container_logs(
                    self.docker_client, tail='all')
                LOG.debug('container log: \n%s', '\n'.join(container_log))
                return service_status.ServiceStatuses.RUNNING
        elif status == "not running":
            return service_status.ServiceStatuses.SHUTDOWN
        elif status == "paused":
            return service_status.ServiceStatuses.PAUSED
        elif status == "exited":
            return service_status.ServiceStatuses.SHUTDOWN
        elif status == "dead":
            return service_status.ServiceStatuses.CRASHED
        else:
            return service_status.ServiceStatuses.UNKNOWN


class RedisApp(service.BaseDbApp):
    _configuration_manager = None

    @property
    def configuration_manager(self):
        if self._configuration_manager:
            return self._configuration_manager

        self._configuration_manager = configuration.ConfigurationManager(
            CONFIG_FILE,
            CONF.database_service_uid,
            CONF.database_service_uid,
            stream_codecs.KeyValueCodec(
                value_quoting=True,
                bool_case=stream_codecs.KeyValueCodec.BOOL_LOWER,
                big_ints=True),
            requires_root=True,
            override_strategy=configuration.ImportOverrideStrategy(
                CNF_INCLUDE_DIR, CNF_EXT)
        )
        return self._configuration_manager

    def __init__(self, status, docker_client):
        super(RedisApp, self).__init__(status, docker_client)

        mount_point = cfg.get_configuration_property('mount_point')
        self.datadir = f"{mount_point}/data/redisdata"

    def get_data_dir(self):
        return self.configuration_manager.get_value('data_directory')

    def set_data_dir(self, value):
        self.configuration_manager.apply_system_override(
            {'data_directory': value})

    def reload(self):
        """
        Ignore
        """
        pass

    def update_overrides(self, overrides):
        """Update config options in the include directory."""
        if overrides:
            self.configuration_manager.apply_user_override(overrides)

    def apply_overrides(self, overrides):
        """Use the 'CONFIG SET' command to apply configuration at runtime.
                Commands that appear multiple times have values separated by a
                white space. For instance, the following two 'save' directives from the
                configuration file...

                    save 900 1
                    save 300 10

                ... would be applied in a single command as:

                    CONFIG SET save "900 1 300 10"

                Note that the 'CONFIG' command has been renamed to prevent
                users from using it to bypass configuration groups.
                """

        for prop_name, prop_args in overrides.items():
            pass

    def start_db(self, update_db=False, ds_version=None, command=None, extra_volumes=None):
        """Start and wait for database service."""
        docker_image = CONF.get(CONF.datastore_manager).docker_image
        image = (f'{docker_image}:latest' if not ds_version else
                 f'{docker_image}:{ds_version}')
        command = command if command else ''

        try:
            redis_pass = self.get_auth_password(file="redis.cnf")
        except exception.UnprocessableEntity:
            redis_pass = utils.generate_random_password()

        # Get uid and gid
        user = "%s:%s" % (CONF.database_service_uid, CONF.database_service_uid)

        # Create folders for postgres on localhost
        for folder in ['/etc/redis', '/var/run/redis']:
            operating_system.ensure_directory(
                folder, user=CONF.database_service_uid,
                group=CONF.database_service_uid, force=True,
                as_root=True)

        volumes = {
            "/etc/redis": {
                "bind": "/etc/redis",
                "mode": "rw"
            },
            "/var/run/redis": {
                "bind": "/var/run/redis",
                "mode": "rw"
            },
            "/var/lib/redis": {
                "bind": "/var/lib/redis",
                "mode": "rw"
            },
            "/bitnami/redis/data": {
                "bind": "/bitnami/redis/data",
                "mode": "rw"
            },
        }

        if extra_volumes:
            volumes.update(extra_volumes)

        # Expose ports
        ports = {}
        tcp_ports = cfg.get_configuration_property('tcp_ports')
        for port_range in tcp_ports:
            for port in port_range:
                ports[f'{port}/tcp'] = port

        try:
            docker_util.start_container(
                self.docker_client,
                image,
                volumes=volumes,
                network_mode="bridge",
                ports=ports,
                user=user,
                environment={
                    "REDIS_PASSWORD": redis_pass,
                },
                command=command
            )

            # Save root password
            LOG.debug("Saving root credentials to local host.")
            self.save_password('redis', redis_pass)
        except Exception:
            LOG.exception("Failed to start database service")
            raise exception.TroveError("Failed to start database service")

        if not self.status.wait_for_status(
            service_status.ServiceStatuses.HEALTHY,
            CONF.state_change_wait_time, update_db
        ):
            raise exception.TroveError("Failed to start database service")

    def restart(self):
        LOG.info("Restarting database")

        # Ensure folders permission for database.
        for folder in ['/etc/redis', '/var/run/redis']:
            operating_system.ensure_directory(
                folder, user=CONF.database_service_uid,
                group=CONF.database_service_uid, force=True,
                as_root=True)

        try:
            docker_util.restart_container(self.docker_client)
        except Exception:
            LOG.exception("Failed to restart database")
            raise exception.TroveError("Failed to restart database")

        if not self.status.wait_for_status(
            service_status.ServiceStatuses.HEALTHY,
            CONF.state_change_wait_time, update_db=True
        ):
            raise exception.TroveError("Failed to start database")

        LOG.info("Finished restarting database")

    def restore_backup(self, context, backup_info, restore_location):
        backup_id = backup_info['id']
        storage_driver = CONF.storage_strategy
        backup_driver = cfg.get_configuration_property('backup_strategy')
        image = cfg.get_configuration_property('backup_docker_image')
        name = 'db_restore'
        volumes = {
            '/bitnami/redis/data': {
                'bind': '/bitnami/redis/data',
                'mode': 'rw'
            }
        }

        os_cred = (f"--os-token={context.auth_token} "
                   f"--os-auth-url={CONF.service_credentials.auth_url} "
                   f"--os-tenant-id={context.project_id}")

        command = (
            f'/usr/bin/python3 main.py --nobackup '
            f'--storage-driver={storage_driver} --driver={backup_driver} '
            f'{os_cred} '
            f'--restore-from={backup_info["location"]} '
            f'--restore-checksum={backup_info["checksum"]} '
            f'--pg-wal-archive-dir {WAL_ARCHIVE_DIR}'
        )
        if CONF.backup_aes_cbc_key:
            command = (f"{command} "
                       f"--backup-encryption-key={CONF.backup_aes_cbc_key}")

        LOG.debug('Stop the database and clean up the data before restore '
                  'from %s', backup_id)
        self.stop_db()
        for dir in [WAL_ARCHIVE_DIR, self.datadir]:
            operating_system.remove_dir_contents(dir)

        # Start to run restore inside a separate docker container
        LOG.info('Starting to restore backup %s, command: %s', backup_id,
                 command)
        output, ret = docker_util.run_container(
            self.docker_client, image, name,
            volumes=volumes, command=command)
        result = output[-1]
        if not ret:
            msg = f'Failed to run restore container, error: {result}'
            LOG.error(msg)
            raise Exception(msg)

        for dir in [WAL_ARCHIVE_DIR, self.datadir]:
            operating_system.chown(dir, CONF.database_service_uid,
                                   CONF.database_service_uid, force=True,
                                   as_root=True)
