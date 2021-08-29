# Copyright (c) 2010 OpenStack Foundation
# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
Scheduler Service
"""

import collections

from oslo_log import log as logging
import oslo_messaging as messaging
from oslo_serialization import jsonutils
from oslo_service import periodic_task
import six
from stevedore import driver

import nova.conf
from nova import exception
from nova import manager
from nova import objects
from nova.objects import host_mapping as host_mapping_obj
from nova import quota
from nova.scheduler.client import report
from nova.scheduler import request_filter
from nova.scheduler import utils

'''MY WORK'''
from openstack import connection
from nova.scheduler import user
from nova.scheduler import server as ser

import keystoneauth1.session
from novaclient import client

'''NOT MY WORK'''
LOG = logging.getLogger(__name__)

CONF = nova.conf.CONF

QUOTAS = quota.QUOTAS

HOST_MAPPING_EXISTS_WARNING = False


class SchedulerManager(manager.Manager):
    """Chooses a host to run instances on."""

    target = messaging.Target(version='4.5')

    _sentinel = object()

    def __init__(self, scheduler_driver=None, *args, **kwargs):
        self.placement_client = report.SchedulerReportClient()
        if not scheduler_driver:
            scheduler_driver = CONF.scheduler.driver
        self.driver = driver.DriverManager(
            "nova.scheduler.driver",
            scheduler_driver,
            invoke_on_load=True).driver
        super(SchedulerManager, self).__init__(service_name='scheduler',
                                               *args, **kwargs)

    @periodic_task.periodic_task(
        spacing=CONF.scheduler.discover_hosts_in_cells_interval,
        run_immediately=True)
    def _discover_hosts_in_cells(self, context):
        global HOST_MAPPING_EXISTS_WARNING
        try:
            host_mappings = host_mapping_obj.discover_hosts(context)
            if host_mappings:
                LOG.info('Discovered %(count)i new hosts: %(hosts)s',
                         {'count': len(host_mappings),
                          'hosts': ','.join(['%s:%s' % (hm.cell_mapping.name,
                                                        hm.host)
                                             for hm in host_mappings])})
        except exception.HostMappingExists as exp:
            msg = ('This periodic task should only be enabled on a single '
                   'scheduler to prevent collisions between multiple '
                   'schedulers: %s' % six.text_type(exp))
            if not HOST_MAPPING_EXISTS_WARNING:
                LOG.warning(msg)
                HOST_MAPPING_EXISTS_WARNING = True
            else:
                LOG.debug(msg)

    @periodic_task.periodic_task(spacing=CONF.scheduler.periodic_task_interval,
                                 run_immediately=True)
    def _run_periodic_tasks(self, context):
        self.driver.run_periodic_tasks(context)

    def reset(self):
        # NOTE(tssurya): This is a SIGHUP handler which will reset the cells
        # and enabled cells caches in the host manager. So every time an
        # existing cell is disabled or enabled or a new cell is created, a
        # SIGHUP signal has to be sent to the scheduler for proper scheduling.
        # NOTE(mriedem): Similarly there is a host-to-cell cache which should
        # be reset if a host is deleted from a cell and "discovered" in another
        # cell.
        self.driver.host_manager.refresh_cells_caches()

    '''MY WORK'''

    def auth_identity(self):
        auth_args = {
            'auth_url': 'ANONYM',
            'project_name': 'ANONYM',
            'username': 'ANONYM',
            'password': 'ANONYM',
            'user_domain_id': 'ANONYM',
            'project_id': 'ANONYM'
        }
        conn = connection.Connection(**auth_args)
        return conn

    def is_user_owner(self, user):
        if user.description is None:
            return False
        else:
            return True

    def get_users_instance_info(self, conn, user_id):
        servers = conn.list_servers()

        users_server = []

        for server in servers:
            if server.user_id == user_id:
                flavor = conn.get_flavor(server.flavor['id'])

                usage = {
                    'disk_gb': flavor.disk,
                    'vcpus': flavor.vcpus,
                    'ram_mb': flavor.ram
                }

                server_obj = ser.Server(host_name=server.host, server_id=server.id, instance_name=server.instance_name,
                                        resource_usage=usage)
                users_server.append(server_obj)
        return users_server

    def get_hosts_from_aggregate(self, aggregate_id, conn):
        aggregate = conn.get_aggregate(aggregate_id)
        hosts = aggregate.hosts

        return hosts

    def possible_server_for_deletion(self, conn, users, all_servers, owner):
        hosts_with_servers = {}
        for owners_host in owner.hosts:
            for server in all_servers:
                if server.user_id != owner.user_id and owners_host == server.host:
                    for user in users:

                        if (server.user_id == user.id and
                                user.description != owner.aggregate_id):

                            if owners_host not in hosts_with_servers:
                                hosts_with_servers[owners_host] = []

                            flavor = conn.get_flavor(server.flavor['id'])

                            usage = {
                                'disk_gb': flavor.disk,
                                'vcpus': flavor.vcpus,
                                'ram_mb': flavor.ram
                            }

                            vm_to_delete = ser.Server(server_id=server.id, host_name=owners_host,
                                                      instance_name=server.instance_name,
                                                      resource_usage=usage)
                            hosts_with_servers[owners_host].append(vm_to_delete)
        return hosts_with_servers

    def users_hypervisors(self, conn, hosts):
        hypervisors = conn.list_hypervisors()

        users_hypervisors = []

        for hypervisor in hypervisors:
            for host in hosts:
                if host == hypervisor.name:
                    users_hypervisors.append(hypervisor)
        return users_hypervisors

    def enough_ressources(self, hypervisors, user_obj, ram_ratio, cpu_ratio):
        for hypervisor in hypervisors:
            if (((hypervisor.memory_free * ram_ratio) - hypervisor.memory_used) >= user_obj.ram_mb_needed and
                    ((hypervisor.vcpus * cpu_ratio) - hypervisor.vcpus_used) >= user_obj.vcpus_needed and
                    (hypervisor.local_disk_free - hypervisor.local_disk_used) >= user_obj.disk_gb_needed):
                return True
        return False

    def select_destinations_for_owner(self, selections, user_obj):
        new_selection_for_owner = []
        all_selections = []

        for selection in selections:
            for sel in selection:
                for users_host in user_obj.hosts:
                    if users_host == sel._obj_nodename:
                        new_selection_for_owner.append(sel)

        all_selections.append(new_selection_for_owner)

        return all_selections

    def delete_servers(self, conn, possible_servers_for_deletion, user_obj):
        for key in possible_servers_for_deletion:
            disk_gb = 0
            vcpus = 0
            ram_mb = 0
            delete_these_servers = []

            servers = possible_servers_for_deletion[key]

            for server in servers:
                disk_gb += server.resource_usage["disk_gb"]
                vcpus += server.resource_usage["vcpus"]
                ram_mb += server.resource_usage["ram_mb"]

                delete_these_servers.append(server)

                if (disk_gb >= user_obj.disk_gb_needed and
                        vcpus >= user_obj.vcpus_needed and
                        ram_mb >= user_obj.ram_mb_needed):

                    for server_to_delete in delete_these_servers:
                        conn.delete_server(name_or_id=server_to_delete.server_id, wait=True, timeout=10)
                    return

    @messaging.expected_exceptions(exception.NoValidHost)
    def select_destinations(self, ctxt, request_spec=None,
                            filter_properties=None, spec_obj=_sentinel, instance_uuids=None,
                            return_objects=False, return_alternates=False):

        connection = self.auth_identity()
        request_user = connection.get_user(spec_obj.user_id)

        enough_ressources = None
        user_is_owner = self.is_user_owner(request_user)

        if user_is_owner:
            hosts_from_aggregate = self.get_hosts_from_aggregate(aggregate_id=request_user.description, conn=connection)
            servers = self.get_users_instance_info(conn=connection, user_id=spec_obj.user_id)

            user_obj = user.User(user_id=spec_obj.user_id, owner=True,
                                 aggregate_id=request_user.description,
                                 hosts=hosts_from_aggregate, servers=servers,
                                 vcpus_needed=spec_obj.flavor.vcpus,
                                 ram_mb_needed=spec_obj.flavor.memory_mb,
                                 disk_gb_needed=spec_obj.flavor.root_gb)

            ram_ratio = CONF.ram_allocation_ratio
            cpu_ratio = CONF.cpu_allocation_ratio

            hypervisors = connection.list_hypervisors()
            enough_ressources = self.enough_ressources(hypervisors, user_obj, ram_ratio, cpu_ratio)

            if not enough_ressources:
                users = connection.list_users()
                all_servers = connection.list_servers()

                servers_for_deletion = self.possible_server_for_deletion(connection, users, all_servers, user_obj)
                LOG.info(servers_for_deletion)
                self.delete_servers(connection, servers_for_deletion, user_obj)

        """Returns destinations(s) best suited for this RequestSpec.

        Starting in Queens, this method returns a list of lists of Selection
        objects, with one list for each requested instance. Each instance's
        list will have its first element be the Selection object representing
        the chosen host for the instance, and if return_alternates is True,
        zero or more alternate objects that could also satisfy the request. The
        number of alternates is determined by the configuration option
        `CONF.scheduler.max_attempts`.

        The ability of a calling method to handle this format of returned
        destinations is indicated by a True value in the parameter
        `return_objects`. However, there may still be some older conductors in
        a deployment that have not been updated to Queens, and in that case
        return_objects will be False, and the result will be a list of dicts
        with 'host', 'nodename' and 'limits' as keys. When return_objects is
        False, the value of return_alternates has no effect. The reason there
        are two kwarg parameters return_objects and return_alternates is so we
        can differentiate between callers that understand the Selection object
        format but *don't* want to get alternate hosts, as is the case with the
        conductors that handle certain move operations.
        """
        LOG.debug("Starting to schedule for instances: %s", instance_uuids)

        # TODO(sbauza): Change the method signature to only accept a spec_obj
        # argument once API v5 is provided.
        if spec_obj is self._sentinel:
            spec_obj = objects.RequestSpec.from_primitives(ctxt,
                                                           request_spec,
                                                           filter_properties)

        is_rebuild = utils.request_is_rebuild(spec_obj)
        alloc_reqs_by_rp_uuid, provider_summaries, allocation_request_version \
            = None, None, None
        if self.driver.USES_ALLOCATION_CANDIDATES and not is_rebuild:
            # Only process the Placement request spec filters when Placement
            # is used.
            try:
                request_filter.process_reqspec(ctxt, spec_obj)
            except exception.RequestFilterFailed as e:
                raise exception.NoValidHost(reason=e.message)

            resources = utils.resources_from_request_spec(
                ctxt, spec_obj, self.driver.host_manager,
                enable_pinning_translate=True)
            res = self.placement_client.get_allocation_candidates(ctxt,
                                                                  resources)
            if res is None:
                # We have to handle the case that we failed to connect to the
                # Placement service and the safe_connect decorator on
                # get_allocation_candidates returns None.
                res = None, None, None

            alloc_reqs, provider_summaries, allocation_request_version = res
            alloc_reqs = alloc_reqs or []
            provider_summaries = provider_summaries or {}

            # if the user requested pinned CPUs, we make a second query to
            # placement for allocation candidates using VCPUs instead of PCPUs.
            # This is necessary because users might not have modified all (or
            # any) of their compute nodes meaning said compute nodes will not
            # be reporting PCPUs yet. This is okay to do because the
            # NUMATopologyFilter (scheduler) or virt driver (compute node) will
            # weed out hosts that are actually using new style configuration
            # but simply don't have enough free PCPUs (or any PCPUs).
            # TODO(stephenfin): Remove when we drop support for 'vcpu_pin_set'
            if (resources.cpu_pinning_requested and
                    not CONF.workarounds.disable_fallback_pcpu_query):
                LOG.debug('Requesting fallback allocation candidates with '
                          'VCPU instead of PCPU')
                resources = utils.resources_from_request_spec(
                    ctxt, spec_obj, self.driver.host_manager,
                    enable_pinning_translate=False)
                res = self.placement_client.get_allocation_candidates(
                    ctxt, resources)
                if res:
                    # merge the allocation requests and provider summaries from
                    # the two requests together
                    alloc_reqs_fallback, provider_summaries_fallback, _ = res

                    alloc_reqs.extend(alloc_reqs_fallback)
                    provider_summaries.update(provider_summaries_fallback)

            if not alloc_reqs:
                LOG.info("Got no allocation candidates from the Placement "
                         "API. This could be due to insufficient resources "
                         "or a temporary occurrence as compute nodes start "
                         "up.")
                raise exception.NoValidHost(reason="")
            else:
                # Build a dict of lists of allocation requests, keyed by
                # provider UUID, so that when we attempt to claim resources for
                # a host, we can grab an allocation request easily
                alloc_reqs_by_rp_uuid = collections.defaultdict(list)
                for ar in alloc_reqs:
                    for rp_uuid in ar['allocations']:
                        alloc_reqs_by_rp_uuid[rp_uuid].append(ar)

        # Only return alternates if both return_objects and return_alternates
        # are True.
        return_alternates = return_alternates and return_objects
        selections = self.driver.select_destinations(ctxt, spec_obj,
                                                     instance_uuids, alloc_reqs_by_rp_uuid, provider_summaries,
                                                     allocation_request_version, return_alternates)
        LOG.info(selections)
        if enough_ressources is not None:
            if not enough_ressources:
                selections = self.select_destinations_for_owner(selections, user_obj)
                LOG.info(selections)
        # If `return_objects` is False, we need to convert the selections to
        # the older format, which is a list of host state dicts.
        if not return_objects:
            selection_dicts = [sel[0].to_dict() for sel in selections]
            return jsonutils.to_primitive(selection_dicts)
        return selections

    def update_aggregates(self, ctxt, aggregates):
        """Updates HostManager internal aggregates information.

        :param aggregates: Aggregate(s) to update
        :type aggregates: :class:`nova.objects.Aggregate`
                          or :class:`nova.objects.AggregateList`
        """
        # NOTE(sbauza): We're dropping the user context now as we don't need it
        self.driver.host_manager.update_aggregates(aggregates)

    def delete_aggregate(self, ctxt, aggregate):
        """Deletes HostManager internal information about a specific aggregate.

        :param aggregate: Aggregate to delete
        :type aggregate: :class:`nova.objects.Aggregate`
        """
        # NOTE(sbauza): We're dropping the user context now as we don't need it
        self.driver.host_manager.delete_aggregate(aggregate)

    def update_instance_info(self, context, host_name, instance_info):
        """Receives information about changes to a host's instances, and
        updates the driver's HostManager with that information.
        """
        self.driver.host_manager.update_instance_info(context, host_name,
                                                      instance_info)

    def delete_instance_info(self, context, host_name, instance_uuid):
        """Receives information about the deletion of one of a host's
        instances, and updates the driver's HostManager with that information.
        """
        self.driver.host_manager.delete_instance_info(context, host_name,
                                                      instance_uuid)

    def sync_instance_info(self, context, host_name, instance_uuids):
        """Receives a sync request from a host, and passes it on to the
        driver's HostManager.
        """
        self.driver.host_manager.sync_instance_info(context, host_name,
                                                    instance_uuids)
# Copyright (c) 2010 OpenStack Foundation
