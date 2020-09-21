# Copyright (c) Microsoft and contributors.  All rights reserved.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import resource_uri_utils
from azure.mgmt.netapp import AzureNetAppFilesManagementClient
from azure.mgmt.netapp.models import NetAppAccount, CapacityPool, Volume, ExportPolicyRule, VolumePropertiesExportPolicy, VolumePropertiesDataProtection, ReplicationObject
from azure.mgmt.resource import ResourceManagementClient
from msrestazure.azure_exceptions import CloudError
from sample_utils import console_output, print_header, get_credentials, resource_exists, wait_for_no_anf_resource, wait_for_anf_resource


# ------------------------------------------IMPORTANT------------------------------------------------------------------
# Setting variables necessary for resources creation - change these to appropriated values related to your environment
# Please NOTE: Resource Group and VNETs need to be created prior to run this code
# ----------------------------------------------------------------------------------------------------------------------

# Primary ANF
PRIMARY_RESOURCE_GROUP_NAME = "[Primary Resource Group Name]"
PRIMARY_LOCATION = "[Primary Location]"
PRIMARY_VNET_NAME = "[Primary VNET Name]"
PRIMARY_SUBNET_NAME = "[Primary Subnet Name]"
PRIMARY_ANF_ACCOUNT_NAME = "[Primary ANF Account Name]"
PRIMARY_CAPACITY_POOL_NAME = "[Primary ANF Capacity Pool Name]"
PRIMARY_VOLUME_NAME = "[Primary ANF Volume Name]"

# Secondary ANF
SECONDARY_RESOURCE_GROUP_NAME = "[Secondary Resource Group Name]"
SECONDARY_LOCATION = "[Secondary Location]"
SECONDARY_VNET_NAME = "[Secondary VNET Name]"
SECONDARY_SUBNET_NAME = "[Secondary Subnet Name]"
SECONDARY_ANF_ACCOUNT_NAME = "[Secondary ANF Account Name]"
SECONDARY_CAPACITY_POOL_NAME = "[Secondary ANF Capacity Pool Name]"
SECONDARY_VOLUME_NAME = "[Secondary ANF Volume Name]"

# Shared ANF Properties
CAPACITY_POOL_SIZE = 4398046511104  # 4TiB which is minimum size
VOLUME_SIZE = 107374182400  # 100GiB - volume minimum size

# Change this to 'True' to enable cleanup process
CLEANUP_RESOURCES = False


def create_account(anf_client, resource_group_name, anf_account_name, location, tags=None):
    """Creates an Azure NetApp Files Account

    Function that creates an Azure NetApp Files Account, which requires building the
    account body object first.

    Args:
        anf_client (AzureNetAppFilesManagementClient): Azure Resource Provider
            Client designed to interact with ANF resources
        resource_group_name (string): Name of the resource group where the
            account will be created
        anf_account_name (string): Name of the Account being created
        location (string): Azure short name of the region where resource will
            be deployed
        tags (object): Optional. Key-value pairs to tag the resource, default
            value is None. E.g. {'cc':'1234','dept':'IT'}

    Returns:
        NetAppAccount: Returns the newly created NetAppAccount resource
    """
    account_body = NetAppAccount(location=location,
                                 tags=tags)

    return anf_client.accounts.create_or_update(account_body,
                                                resource_group_name,
                                                anf_account_name).result()


def create_capacity_pool(anf_client, resource_group_name, anf_account_name,
                         capacity_pool_name, size, location, tags=None):
    """Creates a capacity pool within an account

    Function that creates a Capacity Pool. Capacity pools are needed to define
    maximum service level and capacity.

    Args:
        anf_client (AzureNetAppFilesManagementClient): Azure Resource Provider
            Client designed to interact with ANF resources
        resource_group_name (string): Name of the resource group where the
            capacity pool will be created, it needs to be the same as the
            Account
        anf_account_name (string): Name of the Azure NetApp Files Account where
            the capacity pool will be created
        capacity_pool_name (string): Name of Capacity pool
        service_level (string): Desired service level for this new capacity
            pool, valid values are "Ultra","Premium","Standard"
        size (long): Capacity pool size, values range from 4398046511104
            (4TiB) to 549755813888000 (500TiB)
        location (string): Azure short name of the region where resource will
            be deployed, needs to be the same as the account
        tags (object): Optional. Key-value pairs to tag the resource, default
            value is None. E.g. {'cc':'1234','dept':'IT'}

    Returns:
        CapacityPool: Returns the newly created capacity pool resource
    """
    capacity_pool_body = CapacityPool(location=location,
                                      service_level="Standard",
                                      size=size)

    return anf_client.pools.create_or_update(capacity_pool_body,
                                             resource_group_name,
                                             anf_account_name,
                                             capacity_pool_name).result()


def create_volume(anf_client, resource_group_name, anf_account_name,
                  capacity_pool_name, volume_name, volume_size,
                  subnet_id, location, data_protection=None, tags=None):
    """Creates a volume within a capacity pool

    Function that in this example creates a NFSv4.1 volume within a capacity
    pool, as a note service level needs to be the same as the capacity pool.
    This function also defines the volume body as the configuration settings
    of the new volume.

    Args:
        anf_client (AzureNetAppFilesManagementClient): Azure Resource Provider
            Client designed to interact with ANF resources
        resource_group_name (string): Name of the resource group where the
            volume will be created, it needs to be the same as the account
        anf_account_name (string): Name of the Azure NetApp Files Account where
            the capacity pool holding the volume exists
        capacity_pool_name (string): Capacity pool name where volume will be
            created
        volume_name (string): Volume name
        volume_size (long): Volume size in bytes, minimum value is
            107374182400 (100GiB), maximum value is 109951162777600 (100TiB)
        subnet_id (string): Subnet resource id of the delegated to ANF Volumes
            subnet
        location (string): Azure short name of the region where resource will
            be deployed, needs to be the same as the account
        tags (object): Optional. Key-value pairs to tag the resource, default
            value is None. E.g. {'cc':'1234','dept':'IT'}

    Returns:
        Volume: Returns the newly created volume resource
    """
    rules = [ExportPolicyRule(
        allowed_clients="0.0.0.0/0",
        cifs=False,
        nfsv3=False,
        nfsv41=True,
        rule_index=1,
        unix_read_only=False,
        unix_read_write=True
    )]

    export_policies = VolumePropertiesExportPolicy(rules=rules)

    volume_body = Volume(
        usage_threshold=volume_size,
        creation_token=volume_name,
        location=location,
        service_level="Standard",
        subnet_id=subnet_id,
        protocol_types=["NFSv4.1"],
        export_policy=export_policies,
        data_protection=data_protection
    )

    return anf_client.volumes.create_or_update(volume_body,
                                               resource_group_name,
                                               anf_account_name,
                                               capacity_pool_name,
                                               volume_name).result()


def run_example():
    """Azure NetApp Files Cross-Region Replication (CRR) SDK management example"""

    print_header("Azure NetApp Files Python CRR SDK Sample - Sample "
                 "project that creates a primary ANF Account, Capacity Pool, and an "
                 "NFS v4.1 Volume. Then it creates secondary resources and a "
                 "Data Replication Volume.")

    # Authenticating using service principal, refer to README.md file for requirement details
    credentials, subscription_id = get_credentials()

    console_output("Instantiating a new Azure NetApp Files management client...")
    anf_client = AzureNetAppFilesManagementClient(credentials, subscription_id)
    console_output("Api Version: {}".format(anf_client.api_version))

    console_output("Creating Primary ANF Resources...")
    # Creating ANF Primary Account
    console_output("Creating Primary Account...")

    primary_account = None
    try:
        primary_account = create_account(anf_client,
                                         PRIMARY_RESOURCE_GROUP_NAME,
                                         PRIMARY_ANF_ACCOUNT_NAME,
                                         PRIMARY_LOCATION)

        console_output("\tAccount successfully created. Resource id: {}".format(primary_account.id))
    except CloudError as ex:
        console_output("An error occurred while creating Account: {}".format(ex.message))
        raise

    # Creating Primary Capacity Pool
    console_output("Creating Primary Capacity Pool...")

    primary_capacity_pool = None
    try:
        primary_capacity_pool = create_capacity_pool(anf_client,
                                                     PRIMARY_RESOURCE_GROUP_NAME,
                                                     primary_account.name,
                                                     PRIMARY_CAPACITY_POOL_NAME,
                                                     CAPACITY_POOL_SIZE,
                                                     PRIMARY_LOCATION)

        console_output("\tCapacity Pool successfully created. Resource id: {}".format(primary_capacity_pool.id))
    except CloudError as ex:
        console_output("An error occurred while creating Capacity Pool: {}".format(ex.message))
        raise

    # Creating Primary Volume
    console_output("Creating Primary Volume...")
    primary_subnet_id = '/subscriptions/{}/resourceGroups/{}/providers/Microsoft.Network/virtualNetworks/{}/subnets/{}'.format(
        subscription_id, PRIMARY_RESOURCE_GROUP_NAME, PRIMARY_VNET_NAME, PRIMARY_SUBNET_NAME)

    primary_volume = None
    try:
        pool_name = resource_uri_utils.get_anf_capacity_pool(primary_capacity_pool.id)

        primary_volume = create_volume(anf_client,
                                       PRIMARY_RESOURCE_GROUP_NAME,
                                       primary_account.name,
                                       pool_name,
                                       PRIMARY_VOLUME_NAME,
                                       VOLUME_SIZE,
                                       primary_subnet_id,
                                       PRIMARY_LOCATION)

        console_output("\tVolume successfully created. Resource id: {}".format(primary_volume.id))
    except CloudError as ex:
        console_output("An error occurred while creating Volume: {}".format(ex.message))
        raise

    # Wait for primary volume to be ready
    console_output("Waiting for {} to be available...".format(resource_uri_utils.get_anf_volume(primary_volume.id)))
    wait_for_anf_resource(anf_client, primary_volume.id)

    console_output("Creating Secondary ANF Resources...")
    # Creating ANF Secondary Account
    console_output("Creating Secondary Account...")

    secondary_account = None
    try:
        secondary_account = create_account(anf_client,
                                           SECONDARY_RESOURCE_GROUP_NAME,
                                           SECONDARY_ANF_ACCOUNT_NAME,
                                           SECONDARY_LOCATION)

        console_output("\tAccount successfully created. Resource id: {}".format(secondary_account.id))
    except CloudError as ex:
        console_output("An error occurred while creating Account: {}".format(ex.message))
        raise

    # Creating Secondary Capacity Pool
    console_output("Creating Secondary Capacity Pool...")

    secondary_capacity_pool = None
    try:
        secondary_capacity_pool = create_capacity_pool(anf_client,
                                                       SECONDARY_RESOURCE_GROUP_NAME,
                                                       secondary_account.name,
                                                       SECONDARY_CAPACITY_POOL_NAME,
                                                       CAPACITY_POOL_SIZE,
                                                       SECONDARY_LOCATION)

        console_output("\tCapacity Pool successfully created. Resource id: {}".format(secondary_capacity_pool.id))
    except CloudError as ex:
        console_output("An error occurred while creating Capacity Pool: {}".format(ex.message))
        raise

    # Creating Secondary Volume
    console_output("Creating Secondary Volume...")
    secondary_subnet_id = '/subscriptions/{}/resourceGroups/{}/providers/Microsoft.Network/virtualNetworks/{}/subnets/{}'.format(
        subscription_id, SECONDARY_RESOURCE_GROUP_NAME, SECONDARY_VNET_NAME, SECONDARY_SUBNET_NAME)

    data_replication_volume = None
    try:
        replication_object = ReplicationObject(endpoint_type="dst", remote_volume_region=PRIMARY_LOCATION, remote_volume_resource_id=primary_volume.id, replication_schedule="hourly")
        data_protection_object = VolumePropertiesDataProtection(replication=replication_object)

        pool_name = resource_uri_utils.get_anf_capacity_pool(secondary_capacity_pool.id)

        data_replication_volume = create_volume(anf_client,
                                                SECONDARY_RESOURCE_GROUP_NAME,
                                                secondary_account.name,
                                                pool_name,
                                                SECONDARY_VOLUME_NAME,
                                                VOLUME_SIZE,
                                                secondary_subnet_id,
                                                SECONDARY_LOCATION,
                                                data_protection_object)
        console_output("\tVolume successfully created. Resource id: {}".format(data_replication_volume.id))
    except CloudError as ex:
        console_output("An error occurred while creating Volume: {}".format(ex.message))
        raise

    # Wait for data replication volume to be ready
    console_output("Waiting for {} to be available...".format(resource_uri_utils.get_anf_volume(data_replication_volume.id)))
    wait_for_anf_resource(anf_client, data_replication_volume.id)

    console_output("Authorizing replication in source region...")
    # Authorize replication between the two volumes
    anf_client.volumes.authorize_replication(resource_uri_utils.get_resource_group(primary_account.id),
                                             resource_uri_utils.get_anf_account(primary_account.id),
                                             resource_uri_utils.get_anf_capacity_pool(primary_capacity_pool.id),
                                             resource_uri_utils.get_anf_volume(primary_volume.id),
                                             remote_volume_resource_id=data_replication_volume.id).wait()

    # Wait for replication to initialize on source volume
    wait_for_anf_resource(anf_client, primary_volume.id, replication=True)


    # """
    # Cleanup process. For this process to take effect please change the value of
    # CLEANUP_RESOURCES global variable to 'True'
    # Note: Volume deletion operations at the RP level are executed serially
    # """
    if CLEANUP_RESOURCES:
        # The cleanup process starts from the innermost resources down in the hierarchy chain.
        # In this case: Volumes -> Capacity Pools -> Accounts
        console_output("\tCleaning up resources")

        # Cleaning up volumes
        console_output("Deleting Volumes...")

        try:
            volume_ids = [data_replication_volume.id, primary_volume.id]
            for volume_id in volume_ids:

                resource_group = resource_uri_utils.get_resource_group(volume_id)
                account_name = resource_uri_utils.get_anf_account(volume_id)
                pool_name = resource_uri_utils.get_anf_capacity_pool(volume_id)
                volume_name = resource_uri_utils.get_anf_volume(volume_id)

                # First we need to remove the replication attached to the volume before we can delete the volume itself. We first check if the replication exists and act accordingly
                # Note that we need to delete the replication using the destination volume's id
                # This erases the replication for both destination and source volumes
                try:
                    # This method throws an exception if no replication is found
                    anf_client.volumes.replication_status_method(resource_group,
                                                                 account_name,
                                                                 pool_name,
                                                                 volume_name)

                    anf_client.volumes.delete_replication(resource_group,
                                                          account_name,
                                                          pool_name,
                                                          volume_name).wait()

                    # Wait for replication to finish deleting
                    wait_for_no_anf_resource(anf_client, volume_id, replication=True)
                except CloudError as e:
                    if e.status_code == 404: # If replication is not found then the volume can be safely deleted. Therefore we pass on this error and proceed to delete the volume
                        pass
                    else: # Throw all other exceptions
                        console_output("An error occurred while deleting replication: {}".format(e.message))
                        raise

                anf_client.volumes.delete(resource_group,
                                          account_name,
                                          pool_name,
                                          volume_name).wait()

                # ARM workaround to wait for the deletion to complete
                wait_for_no_anf_resource(anf_client, volume_id)
                console_output("\tSuccessfully deleted Volume {}".format(volume_id))
        except CloudError as ex:
            console_output("An error occurred while deleting volumes: {}".format(ex.message))
            raise

        # Cleaning up capacity pools
        console_output("Deleting Capacity Pools...")

        try:
            pool_ids = [primary_capacity_pool.id, secondary_capacity_pool.id]
            for pool_id in pool_ids:

                resource_group = resource_uri_utils.get_resource_group(pool_id)
                account_name = resource_uri_utils.get_anf_account(pool_id)
                pool_name = resource_uri_utils.get_anf_capacity_pool(pool_id)

                anf_client.pools.delete(resource_group,
                                        account_name,
                                        pool_name).wait()

                # ARM workaround to wait for the deletion to complete
                wait_for_no_anf_resource(anf_client, pool_id)
                console_output("\tSuccessfully deleted Capacity Pool {}".format(pool_id))
        except CloudError as ex:
            console_output("An error occurred while deleting capacity pools: {}".format(ex.message))
            raise

        # Cleaning up accounts
        console_output("Deleting Accounts...")

        try:
            account_ids = [primary_account.id, secondary_account.id]
            for account_id in account_ids:

                resource_group = resource_uri_utils.get_resource_group(account_id)
                account_name = resource_uri_utils.get_anf_account(account_id)

                anf_client.accounts.delete(resource_group,
                                           account_name).wait()

                # ARM workaround to wait for the deletion to complete
                wait_for_no_anf_resource(anf_client, account_id)
                console_output("\tSuccessfully deleted Account {}".format(account_id))
        except CloudError as ex:
            console_output("An error occurred while deleting accounts: {}".format(ex.message))
            raise

    console_output("ANF Cross-Region Replication has completed successfully")


if __name__ == "__main__":
    run_example()
