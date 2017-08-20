# Copyright 2012 Yelp and Contributors
# Copyright 2013 Lyft
# Copyright 2014 Brett Gibson
# Copyright 2015-2016 Yelp
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Utilities related to cluster pooling. This code used to be in mrjob.emr.

In theory, this module might support pooling in general, but so far, there's
only a need for pooling on EMR.

"""
from collections import defaultdict
from datetime import timedelta
from logging import getLogger

from mrjob.aws import _boto3_now
from mrjob.aws import EC2_INSTANCE_TYPE_TO_COMPUTE_UNITS
from mrjob.aws import EC2_INSTANCE_TYPE_TO_MEMORY

log = getLogger(__name__)


### current versions of these functions, using "cluster" API calls ###

# these are "hidden" because there's no need to access them directly

def _est_time_to_hour(cluster_summary, now=None):
    """How long before job reaches the end of the next full hour since it
    began. This is important for billing purposes.

    If it happens to be exactly a whole number of hours, we return
    one hour, not zero.
    """
    if now is None:
        now = _boto3_now()

    timeline = cluster_summary.get('Status', {}).get('Timeline', {})

    creationdatetime = timeline.get('CreationDateTime')

    if not creationdatetime:
        # do something reasonable if creationdatetime isn't set
        return timedelta(minutes=60)

    run_time = now - creationdatetime
    return timedelta(seconds=((-run_time).seconds % 3600.0 or 3600.0))


def _pool_tags(hash, name):
    """Return a dict with "hidden" tags to add to the given cluster."""
    return dict(__mrjob_pool_hash=hash, __mrjob_pool_name=name)


def _extract_tags(cluster):
    """Pull the tags from a cluster, as a dict."""
    return {t['Key']: t['Value'] for t in cluster.get('Tags') or []}


def _pool_hash_and_name(cluster):
    """Return the hash and pool name for the given cluster, or
    ``(None, None)`` if it isn't pooled."""
    tags = _extract_tags(cluster)
    return tags.get('__mrjob_pool_hash'), tags.get('__mrjob_pool_name')


def _legacy_pool_hash_and_name(bootstrap_actions):
    """Get pool hash and name from a pre-v0.6.0 job."""
    for ba in bootstrap_actions:
        if ba['Name'] == 'master':
            args = ba['Args']
            if len(args) == 2 and args[0].startswith('pool-'):
                return args[0][5:], args[1]

    return None, None


### instance groups ###

def _instance_groups_satisfy(actual_igs, requested_igs):
    """Do the actual instance groups from a cluster satisfy the requested
    ones, for the purpose of pooling?

    The formats are slightly different; the format of *requested_igs* is here:
    http://docs.aws.amazon.com/ElasticMapReduce/latest/API/API_InstanceGroup.html
    and the format of *actual_igs* is here:
    http://docs.aws.amazon.com/ElasticMapReduce/latest/API/API_ListInstanceGroups.html
    """
    # a is a map from role to actual instance groups
    a = defaultdict(list)
    for ig in actual_igs:
        a[ig['InstanceGroupType'].lower()].append(ig)

    # r is a map from role to request (should be only one per role)
    r = {req['InstanceRole'].lower(): req for req in requested_igs}

    # updated request to account for extra instance groups
    # see #1630 for what we do when roles don't match
    if set(a) - set(r):
        r = _add_missing_roles_to_request(set(a) - set(r), r,
                                          ['InstanceCount'])

    if set(a) != set(r):
        log.debug("    missing instance group roles")
        return None

    sort_keys = {}
    for role in sorted(r):
        sort_key = _igs_for_same_role_satisfy(a[role], r[role])
        if not sort_key:  # doesn't satisfy
            return None
        sort_keys[role] = sort_key

    return tuple(sort_keys.get(role) for role in ('core', 'task', 'master'))


def _igs_for_same_role_satisfy(actual_igs, requested_ig):
    """Does the *actual* list of instance groups satisfy the *requested*
    one?
    """
    # bid price/on-demand
    if not all(_ig_satisfies_bid_price(ig, requested_ig) for ig in actual_igs):
        return None

    # memory
    if not all(_ig_satisfies_mem(ig, requested_ig) for ig in actual_igs):
        return None

    # EBS volumes
    if not all(_ebs_satisfies(ig, requested_ig) for ig in actual_igs):
        return None

    # CPU (this returns # of compute units or None)
    return _igs_satisfy_cpu(actual_igs, requested_ig)


def _ig_satisfies_bid_price(actual_ig, requested_ig):
    """Does the actual instance group definition satisfy the bid price
    (or lack thereof) of the requested instance group?
    """
    # on-demand instances satisfy every bid price
    if actual_ig['Market'] == 'ON_DEMAND':
        return True

    if requested_ig.get('Market', 'ON_DEMAND') == 'ON_DEMAND':
        log.debug('    spot instance, requested on-demand')
        return False

    if actual_ig['BidPrice'] == requested_ig.get('BidPrice'):
        return True

    try:
        if float(actual_ig['BidPrice']) >= float(requested_ig.get('BidPrice')):
            return True
        else:
            # low bid prices mean cluster is more likely to be
            # yanked away
            log.debug('    bid price too low')
            return False
    except ValueError:
        log.debug('    non-float bid price')
        return False


def _ig_satisfies_mem(actual_ig, requested_ig):
    """Does the actual instance group satisfy the memory requirements of
    the requested instance group?"""
    actual_type = actual_ig['InstanceType']
    requested_type = requested_ig['InstanceType']

    # this works even for unknown instance types
    if actual_type == requested_type:
        return True

    try:
        if (EC2_INSTANCE_TYPE_TO_MEMORY[actual_type] >=
                EC2_INSTANCE_TYPE_TO_MEMORY[requested_type]):
            return True
        else:
            log.debug('    too little memory')
            return False
    except KeyError:
        log.debug('    unknown instance type')
        return False


def _igs_satisfy_cpu(actual_igs, requested_ig):
    """Does the list of actual instance groups satisfy the CPU requirements
    of the requested instance group?

    If so, return the number of compute units. Otherwise, return ``None``

    If the requested instance type is unknown, just return the number
    of actual instances of the same type.
    """
    requested_type = requested_ig['InstanceType']
    num_requested = requested_ig['InstanceCount']

    # count number of compute units (cu)
    if requested_type in EC2_INSTANCE_TYPE_TO_COMPUTE_UNITS:
        requested_cu = (
            num_requested * EC2_INSTANCE_TYPE_TO_COMPUTE_UNITS[requested_type])

        # don't require instances to be running; we'd be worse off if
        # we started our own cluster from scratch. (This can happen if
        # the previous job finished while some task instances were
        # still being provisioned.)
        actual_cu = sum(
            ig['RequestedInstanceCount'] *
            EC2_INSTANCE_TYPE_TO_COMPUTE_UNITS.get(ig['InstanceType'], 0.0)
            for ig in actual_igs)
    else:
        # unknown instance type, just count # of matching instances
        requested_cu = num_requested
        actual_cu = sum(ig['RequestedInstanceCount'] for ig in actual_igs
                         if ig['InstanceType'] == requested_type)

    if actual_cu >= requested_cu:
        return actual_cu
    else:
        log.debug('    not enough compute units')
        return None



### instance fleets ###

def _instance_fleets_satisfy(actual_fleets, req_fleets):
    """Common code for :py:func:`
    :py:func:`_instance_groups_satisfy_fleets` and
    :py:func:`_instance_groups_satisfy`."""

    # a is a map from role to actual instance fleet
    # (unlike with groups, there can never be more than one fleet per role)
    a = {f['InstanceFleetType'].lower(): f for f in actual_fleets}

    # r is a map from role to request (should be only one per role)
    r = {f['InstanceFleetType'].lower(): f for f in req_fleets}

    # updated request to account for extra instance groups
    # see #1630 for what we do when roles don't match
    if set(a) - set(r):
        r = _add_missing_roles_to_request(
            set(a) - set(r), r,
            ['TargetOnDemandCapacity', 'TargetSpotCapacity'])

    if set(a) != set(r):
        log.debug("    missing instance fleet roles")
        return None

    sort_keys = {}
    for role in sorted(r):
        sort_key = _fleet_for_same_role_satisfies(a[role], r[role])
        if not sort_key:  # doesn't satisfy
            return None
        sort_keys[role] = sort_key

    return tuple(sort_keys.get(role) for role in ('core', 'task', 'master'))


def _fleet_for_same_role_satisfies(actual_fleet, req_fleet):
    # match up instance types
    actual_specs = {spec['InstanceType']: spec
                    for spec in actual_fleet['InstanceTypeSpecifications']}
    req_specs = {spec['InstanceType']: spec
                 for spec in req_fleet['InstanceTypeConfigs']}

    if set(actual_specs) - set(req_specs):
        log.debug('    fleet may include wrong instance types')
        return

    if not all(_fleet_spec_satsifies(actual_specs[t], req_specs[t])
               for t in actual_specs):
        return

    # capacity
    actual_on_demand = actual_fleet.get('ProvisionedOnDemandCapacity', 0)
    req_on_demand = req_fleet.get('RequestedOnDemandCapacity', 0)

    if req_on_demand > actual_on_demand:
        log.debug('    not enough on-demand capacity')
        return

    actual_spot = actual_fleet.get('ProvisionedSpotCapacity', 0)
    req_spot = req_fleet.get('RequestedSpotCapacity', 0)

    # allow extra on-demand instances to serve as spot instances
    if req_spot > actual_spot + (actual_on_demand - req_on_demand):
        log.debug('    not enough spot capacity')

    # handle TERMINATE_CLUSTER timeout action. This really doesn't play
    # well with pooling anyhow
    if _get_timeout_action(actual_fleet) == 'TERMINATE_CLUSTER':
        if _get_timeout_action(req_fleet) != 'TERMINATE_CLUSTER':
            log.debug('    self-terminating fleet not requested')
            return

        if ((actual_spot < actual_fleet.get('TargetSpotCapacity', 0)) and
            (_get_timeout_duration(actual_fleet) <
             _get_timeout_duration(req_fleet))):
            log.debug('    fleet may self-terminate prematurely')
            return

    # sort by total capacity, with on-demand as a tie-breaker
    return (actual_on_demand + actual_spot, actual_on_demand)


def _get_timeout_action(fleet):
    return fleet.get(
        'LaunchSpecifications', {}).get(
            'SpotSpecification', {}).get(
                'TimeoutAction')


def _get_timeout_duration(fleet):
    return fleet.get(
        'LaunchSpecifications', {}).get(
            'SpotSpecification', {}).get(
                'TimeoutAction', 0.0)


def _fleet_spec_satsifies(actual_spec, req_spec):
    """Make sure the specification for the given instance type is as
    good or better than the requested spec.

    Specs must have the same weight, but "better" EBS configurations are
    accepted.

    Bid price must either be higher or the *actual* bid price
    must be same as on-demand
    """
    if (actual_spec.get('WeightedCapacity', 1) !=
            req_spec.get('WeightedCapacity', 1)):
        log.debug('    different weighted capacity for same instance type')
        return

    if not _ebs_satisfies(actual_spec, req_spec):
        return

    # bid price is the max, don't worry about it
    if actual_spec.get('BidPriceAsPercentageOfOnDemandPrice', 100) >= 100:
        return True

    # absolute bid price
    req_bid_price = req_spec.get('BidPrice')
    if req_bid_price is not None:
        actual_bid_price = actual_spec.get('BidPrice')
        if actual_bid_price is None:
            log.debug('    no bid price specified')
            return

        try:
            if not float(actual_bid_price) >= float(req_bid_price):
                log.debug('    bid price too low')
                return
        except TypeError:
            log.debug('    non-numeric bid price')
            return

    # relative bid price
    req_bid_percent = req_spec.get('BidPriceAsPercentageOfOnDemandPrice')
    if req_bid_percent:
        actual_bid_percent = actual_spec.get(
            'BidPriceAsPercentageOfOnDemandPrice')
        if actual_bid_percent is None:
            log.debug('    no bid price as % of on-demand price')
            return

        if req_bid_percent > actual_bid_percent:
            log.debug('    bid price as % of on-demand price too low')
            return

    return True


### common code for matching instance (groups or fleets) ###


def _add_missing_roles_to_request(
        missing_roles, role_to_req, req_count_fields):
    """Helper for :py:func:`_igs_satisfy_request`. Add requests for
    *missing_roles* to *role_to_ig* so that we have a better chance of
    matching the cluster's actual instance groups."""
    # see #1630 for discussion

    # don't worry about modifying *role_to_req*; this is
    # a helper func

    if 'core' in missing_roles and list(role_to_req) == ['master']:
        # both core and master have to satisfy master-only request
        role_to_req['core'] = role_to_req['master']

    if 'task' in missing_roles and 'core' in role_to_req:
        # make sure tasks won't crash on the task instances,
        # but don't require the same amount of CPU
        role_to_req['task'] = dict(role_to_req['core'])
        for req_count_field in req_count_fields:
            role_to_req['task'][req_count_field] = 0

    return role_to_req


def _ebs_satisfies(actual, request):
    """Does *actual* have EBS volumes that satisfy *request*.

    *actual* is either an instance group from ``ListInstanceGroups``
    or an instance fleet spec from ``ListInstanceFleets`` (format
    is the same).

    *request* is either the ``InstanceGroups`` or ``InstanceFleets``
    param to ``RunJobFlow``

    If *request* doesn't have an EBS Configuration, we return
    True.

    If *request* requests EBS optimization, *actual* should provide it.

    Finally, *actual* should have the same or better block devices
    as those in *request* (same volume type, at least as much IOPS
    and volume size).
    """
    req_ebs_config = request.get('EbsConfiguration')

    if not req_ebs_config:
        return True

    if (req_ebs_config.get('EbsOptimized')
            and not actual.get('EbsOptimized')):
        log.debug('    need EBS-optimized instances')
        return False

    req_device_configs = req_ebs_config.get('EbsBlockDeviceConfigs')

    if not req_device_configs:
        return True

    req_volumes = []

    for req_device_config in req_device_configs:
        volume = req_device_config['VolumeSpecification']
        num_volumes = req_device_config.get('VolumesPerInstance', 1)

        req_volumes.extend([volume] * num_volumes)

    actual_volumes = [
        bd.get('VolumeSpecification', {})
        for bd in actual.get('EbsBlockDevices', [])]

    return _ebs_volumes_satisfy(actual_volumes, req_volumes)


def _ebs_volumes_satisfy(actual_volumes, req_volumes):
    """Does the given list of actual EBS volumes satisfy the given request?

    Just compare them one by one (we want each actual device to be
    bigger/faster; just having the same amount of capacity or iops
    isn't enough).
    """
    if len(req_volumes) > len(actual_volumes):
        log.debug('    more EBS volumes requested than available')
        return False

    return all(_ebs_volume_satisfies(a, r)
               for a, r in zip(actual_volumes, req_volumes))


def _ebs_volume_satisfies(actual_volume, req_volume):
    """Does the given actual EBS volume satisfy the given request?"""
    if req_volume.get('VolumeType') != actual_volume.get('VolumeType'):
        log.debug('    wrong EBS volume type')
        return False

    if not req_volume.get('SizeInGB', 0) <= actual_volume.get('SizeInGB', 0):
        log.debug('    EBS volume too small')
        return False

    # Iops isn't really "optional"; it has to be set if volume type is
    # io1 and not set otherwise
    if not (req_volume.get('Iops', 0) <= actual_volume.get('Iops', 0)):
        log.debug('    EBS volume too slow')
        return False

    return True
