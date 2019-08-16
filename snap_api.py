#!/usr/bin/env python

"""
Snapshot api

"""


from flask import Flask, escape, request
from flask_restplus import Api, Resource, reqparse
from pyVim.connect import SmartConnectNoSSL, Disconnect
from vm_sn_op import *


app = Flask(__name__)
api = Api(app,
          version="1.0",
          title="Snapshot as a Service",
          description="Manage  the snapshot of VM")


class Connection:
    """
    Connecton class

    """
    def __init__(self, vcname):
        try:
            self.si = SmartConnectNoSSL(host=vcname, user = "svc.scriptuser@vmware.com", pwd = "scripta11j0bs$", port = 443)
        except IOError as e:
            print(e)

    
    def get_vm(self, vmname):
        """
        Get the VM object
        """
        vms = get_obj(self.si, self.si.content.rootFolder, [vim.VirtualMachine]) 

        filter_propertys = ["name"]
        filter_value = vmname
        vm_obj = Filter_VM(self.si, vms, filter_propertys, filter_value)

        return vm_obj


    @staticmethod
    def view_all_snapshot(vm):

        """ View the Tree of Snapshots"""
        snapshot_info = []

        if vm.snapshot:
            snap_info = vm.snapshot

            tree = snap_info.rootSnapshotList
            while tree[0].childSnapshotList is not None:
                
                vm_snapshot = {"name" : tree[0].name,
                               "create_time" : str(tree[0].createTime),
                               "snapshot_state" : tree[0].state,
                               "description" : tree[0].description }

                snapshot_info.append(vm_snapshot)

                if len(tree[0].childSnapshotList) < 1:
                    break
                tree = tree[0].childSnapshotList

        return snapshot_info


    @staticmethod
    def view_current_snapshot(vm):

        """Helps to view the current snapshot"""


        if vm.snapshot:
            current_snapref = vm.snapshot.currentSnapshot
            current_snap_obj = get_current_snap_obj(
                vm.snapshot.rootSnapshotList, current_snapref)
            vm_snapshot = {"name" : current_snap_obj[0].name,
                           "create_time" : str(current_snap_obj[0].createTime),
                           "snapshot_state" : current_snap_obj[0].state,
                           "description" : current_snap_obj[0].description}

        return vm_snapshot
       

    @staticmethod
    def create_snapshot(si, vm_obj, sn_name, description=None, sn_memory=None, sn_quiesce=None):

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

        #Test the condition to be met before taking the snapshot.
        test_condition = check_condition(si, vm_obj)
        
        if test_condition:
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
        else:
            print("###Snapshot is not taken due to the violation of the condition.")
    

    def __del__(self):
        Disconnect(self.si)


#My parsers
parser = reqparse.RequestParser()
parser.add_argument('vcname',  required=True,  help='vCenter to connect to')
parser.add_argument('vmname', required=True)
parser.add_argument('snapshot_name',required=True)

@api.route('/snapshot', endpoint = "snapshot")
@api.doc(params = { 'vcname' : 'vCenter Name', 'vmname' : 'Virtual Machine Name' } )
class Snapshot(Resource):
    """
    Snapshot Template
    """

    def get(self):
        """
        Get method return
        """
        parser = reqparse.RequestParser()
        parser.add_argument('vcname',  help='vCenter to connect to')
        parser.add_argument('vmname')
        args = parser.parse_args()

        vc_connect = Connection(args.vcname)
        vm_obj = vc_connect.get_vm(args.vmname)

        return Connection.view_all_snapshot(vm_obj)
    

    @api.expect(parser, validate=True)
    def post(self):
        """
        Post operations
        """

        args = parser.parse_args()

        vc_connect = Connection(args.vcname)
        vm_obj = vc_connect.get_vm(args.vmname)

        #perform the creation of snapshot operation
        Connection.create_snapshot(vc_connect.si, vm_obj, args.snapshot_name)

        #View the recent snapshot! Most obviously that will be the one the that I created just before... But, this is not good method. 
        #Later modify in such a way that after creation you need to get the snapshot info that is created...
        Connection.view_current_snapshot(vm_obj)




        

    
       


        


        
        
