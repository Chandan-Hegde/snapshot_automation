#!/usr/bin/env python
#
# Inspired from pyvmomi community samples
#
#
from __future__ import print_function

from pyVmomi import vim
from pyVmomi import vmodl
import argparse
import getpass
from pyVim.connect import SmartConnectNoSSL, Disconnect
import atexit
import sys


def get_args():

    """Get command line args from the user """

    parser = argparse.ArgumentParser(
        description='Standard Arguments for talking to vCenter')

    # because -h is reserved for 'help' we use -s for service
    parser.add_argument('-s', '--host',
                        required=True,
                        action='store',
                        help='vSphere service to connect to')

    # because we want -p for password, we use -o for port
    parser.add_argument('-o', '--port',
                        type=int,
                        default=443,
                        action='store',
                        help='Port to connect on')

    parser.add_argument('-u', '--user',
                        required=True,
                        action='store',
                        help='User name to use when connecting to host')

    parser.add_argument('-p', '--password',
                        required=False,
                        action='store',
                        help='Password to use when connecting to host')

    parser.add_argument('-vm', '--vmname',
                        required=False,
                        action='store',
                        help='VM Name whose performance data needs to be retrieved')

    parser.add_argument('-d', '--description', required=False,
                        help="Description for the snapshot")

    parser.add_argument('-n', '--name', required=False,
                        help="Name for the Snapshot")

    parser.add_argument('-memory', required=False,
                        help="Memory of snapshot boolean value",
                        choices=('yes', 'no'),
                        default=False)

    parser.add_argument('-quiesce', required=False,
                        help="Quiesce boolean",
                        choices=('yes', 'no'),
                        default=False)

    parser.add_argument('-action', required=True,
                        choices=("create", "delete", "list_all", "list_current", "delete", "revert","delete_all"))

    parser.add_argument('-snapshotname', required=False,
                        help="Snapshot Name to which you want to revert or delete the one")

    parser.add_argument('-child_snapshot_delete', required=False,
                        help="To decide if the child snapshot to be deleted while you delete the parent Snapshot",
                        choices=('yes','no'),
                        default='no')

    args = parser.parse_args()

    if not args.password:
        args.password = getpass.getpass(
            prompt='Enter password for host %s and user %s: ' %
                   (args.host, args.user))
    return args

def wait_for_tasks(service_instance, tasks):
    """Given the service instance si and tasks, it returns after all the
   tasks are complete
   """
    property_collector = service_instance.content.propertyCollector
    task_list = [str(task) for task in tasks]
    # Create filter
    obj_specs = [vmodl.query.PropertyCollector.ObjectSpec(obj=task)
                 for task in tasks]
    property_spec = vmodl.query.PropertyCollector.PropertySpec(type=vim.Task,
                                                               pathSet=[],
                                                               all=True)
    filter_spec = vmodl.query.PropertyCollector.FilterSpec()
    filter_spec.objectSet = obj_specs
    filter_spec.propSet = [property_spec]
    pcfilter = property_collector.CreateFilter(filter_spec, True)
    try:
        version, state = None, None
        # Loop looking for updates till the state moves to a completed state.
        while len(task_list):
            update = property_collector.WaitForUpdates(version)
            for filter_set in update.filterSet:
                for obj_set in filter_set.objectSet:
                    task = obj_set.obj
                    for change in obj_set.changeSet:
                        if change.name == 'info':
                            state = change.val.state
                        elif change.name == 'info.state':
                            state = change.val
                        else:
                            continue

                        if not str(task) in task_list:
                            continue

                        if state == vim.TaskInfo.State.success:
                            # Remove task from taskList
                            task_list.remove(str(task))
                        elif state == vim.TaskInfo.State.error:
                            raise task.info.error
            # Move to next version
            version = update.version
    finally:
        if pcfilter:
            pcfilter.Destroy()


def get_obj(content, vimtype, name):

    """Create container view and search for object in it"""
    obj = None
    container = content.viewManager.CreateContainerView(
        content.rootFolder, vimtype, True)
    for c in container.view:
        if name:
            if c.name == name:
                obj = c
                break
        else:
            obj = c
            break

    container.Destroy()
    return obj


def create_snapshot(si, vm_obj, sn_name, description, sn_memory, sn_quiesce):

    """ Creates Snapshot given the VM object and Snapshot name and necessary description"""

    desc = None
    if description:
        desc = description

    if sn_memory == 'yes':
        sn_memory = True
    else:
        sn_memory = False

    if sn_quiesce == 'yes':
        sn_quiesce = True
    else:
        sn_quiesce = False

    task = vm_obj.CreateSnapshot_Task(name=sn_name,
                                      description=desc,
                                      memory=sn_memory,
                                      quiesce=sn_quiesce)
    print("Creating Snapshot {} on VM {}".format(sn_name, vm_obj.name))
    wait_for_tasks(si, [task])

    if sn_memory:
        print("Snapshot {} is taken on VM {} with memory".format(sn_name, vm_obj.name))
    else:
        print("Snapshot {} is taken on VM {} with no in-memory".format(sn_name, vm_obj.name))


def view_all_snapshot(vm):

    """ View the Tree of Snapshots"""

    if vm.snapshot:
        snap_info = vm.snapshot

        tree = snap_info.rootSnapshotList
        while tree[0].childSnapshotList is not None:
            print("Name : {0}     ===>    Created Time : {1} | Snapshot State : {2} | Description : {3}".format(
                tree[0].name, tree[0].createTime, tree[0].state, tree[0].description))
            if len(tree[0].childSnapshotList) < 1:
                break
            tree = tree[0].childSnapshotList
    else:
        print("No Snapshots found for VM {}".format(vm.name))


def get_current_snap_obj(snapshots, snapob):
    snap_obj = []
    for snapshot in snapshots:
        if snapshot.snapshot == snapob:
            snap_obj.append(snapshot)
        snap_obj = snap_obj + get_current_snap_obj(
                                snapshot.childSnapshotList, snapob)
    return snap_obj


def view_current_snapshot(vm):

    """Helps to view the current snapshot"""

    if vm.snapshot:
        current_snapref = vm.snapshot.currentSnapshot
        current_snap_obj = get_current_snap_obj(
            vm.snapshot.rootSnapshotList, current_snapref)
        current_snapshot = "Name: %s ===>  Description: %s | " \
                           "CreateTime: %s | State: %s" % (
                               current_snap_obj[0].name,
                               current_snap_obj[0].description,
                               current_snap_obj[0].createTime,
                               current_snap_obj[0].state)
        print("Virtual machine %s current snapshot is:" % vm.name)
        print(current_snapshot)
    else:
        print("No Snapshot found for VM {}".format(vm.name))


def get_snapshots_by_name_recursively(snapshots, snapname):

    snap_obj = []

    for snapshot in snapshots:
        if snapshot.name == snapname:
            snap_obj.append(snapshot)
        else:
            snap_obj = snap_obj + get_snapshots_by_name_recursively(
                                    snapshot.childSnapshotList, snapname)
    return snap_obj


def delete_snapshot(si, vm, snapshot_name, child_snapshot_delete):

    """Delete the Snapshot sn of vm"""

    snap_obj = get_snapshots_by_name_recursively(
        vm.snapshot.rootSnapshotList, snapshot_name)

    if len(snap_obj) == 1:
        snap_obj = snap_obj[0].snapshot

        if child_snapshot_delete == "yes":
            print("Removing snapshot {} along with the child snapshots".format(snapshot_name))
            task = snap_obj.RemoveSnapshot_Task(True) #Remove child snapshot as well
            wait_for_tasks(si, [task])

        if child_snapshot_delete == "no":
            print("Removing snapshot {} only of VM {}".format(snapshot_name, vm.name))
            task = snap_obj.RemoveSnapshot_Task(False) #Does not remove the chile snapshot
            wait_for_tasks(si, [task])

    else:
        print("No snapshots found with name: %s on VM: %s" % (snapshot_name, vm.name))


def revert_snapshot(si, vm, snapshot_name):

    """ Revert VM to snapshot sn"""

    snap_obj = get_snapshots_by_name_recursively(
        vm.snapshot.rootSnapshotList, snapshot_name)

    if len(snap_obj) == 1:
        snap_obj = snap_obj[0].snapshot
        print("Reverting to snapshot %s" % snapshot_name)
        task = snap_obj.RevertToSnapshot_Task()
        wait_for_tasks(si, [task])
    else:
        print("No snapshots found with name: %s on VM: %s" % (snapshot_name, vm.name))


def main():

    args = get_args()

    si = None

    # Connect to the host without SSL signing
    try:
        si = SmartConnectNoSSL(
            host=args.host,
            user=args.user,
            pwd=args.password,
            port=int(args.port))
        atexit.register(Disconnect, si)

    except IOError as e:
        pass

    if not si:
        raise SystemExit("Unable to connect to host with supplied info.")

    vm_obj = get_obj(si.content, [vim.VirtualMachine], args.vmname)

    if vm_obj is None:
        raise SystemExit("Unable to locate VirtualMachine.")

    if args.action == "create":
        if args.name is None:
            print("The Snapshot Name must be specified. -n <snapshot_name> or -name<snap_name>")
        else:
            create_snapshot(si, vm_obj, args.name, args.description, args.memory, args.quiesce)

    elif args.action == "list_all":
        view_all_snapshot(vm_obj)

    elif args.action == "list_current":
        view_current_snapshot(vm_obj)

    elif args.action == "delete":
        if args.snapshotname is None:
            print("Please specify the snapshot name that you want to delete with parameter -snapshotname")
            exit(1)
        delete_snapshot(si, vm_obj, args.snapshotname, args.child_snapshot_delete)

    elif args.action == "revert":
        if args.snapshotname is None:
            print("Please specify the snapshot name that you want to revert to with parameter -snapshotname")
            exit(1)
        revert_snapshot(si, vm_obj, args.snapshotname)

    elif args.action == "delete_all":
        print("Removing all snapshots for virtual machine %s" % vm_obj.name)
        task = vm_obj.RemoveAllSnapshots()
        wait_for_tasks(si, [task])
        print("All Snapshots of the VM {} is removed".format(vm_obj.name))

    else:
        print("Invalid Operation")

    del vm_obj


if __name__ == "__main__":
    main()
