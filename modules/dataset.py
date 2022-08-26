#!/usr/bin/env python3
__metaclass__ = type
from dataclasses import dataclass
from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.basic import env_fallback

if True:
    from ..module_utils import api_datasets
    from ..module_utils import api

from ansible.module_utils import api
from ansible.module_utils import api_datasets

# from ansible.module_utils import basic

DOCUMENTATION = r"""
---
module: dataset

short_description: Enables management of datasets 

# If this is part of a collection, you need to use semantic versioning,
# i.e. the version is of the form "2.5.0" and not "2.4".
version_added: "1.0.0"

description: This module enables provisioning and removal of datasets. A
dataset is a core element which enables data to be stored on the BrickStor and
shared across multiple users over protocols like SMB and NFS. This module does
not provide the capability to share datasets, but does manage there existence
and their various attributes.


options:
    host:
        description: Address of the BrickStor HTTP API server.
        required: true
        type: str
    port:
        description: TLS port of the BrickStor HTTP API server.
        required: false
        type: int
    username:
        description: Name of the user with which to authenticate to the HTTP API.
        required: false
        type: str
    password:
        description: Password with which to authenticate to the HTTP API.
        required: false
        type: str
    ds_path:
        description: Dataset path; e.g.: p01/global/mydataset.
        required: true
        type: str
    storage_profile:
        description: Storage profile which should be assigned at the time of filesystem creation.
        required: false
        type: str
    recursive:
        description:
            - Enables recursive operation on datasets. 
            - Currently only applicable do destroys, where removal of a dataset may require removal of children.
        required: false
        type: bool
    aclmode:
        description: ACL mode setting on the dataset.
        required: false
        type: str
    aclinherit:
        description: ACL inheritance setting on the dataset.
        required: false
        type: str
    atime:
        description:
            - Controls atime behavior on the dataset.
            - When disabled, access times are not modified on files/directories.
        required: false
    quota:
        description:
            - Limits the amount of usable space by this dataset and its descendents.
            - Input may be numeric, e.g.: 1024000 which is in bytes.
            - Input may be alpha-numeric, e.g.: 1M, 1G, 1T.
        required: false
        type: str
    refquota:
        description:
            - Limits the amount of usable space by this dataset not including its descendents.
            - Input may be numeric, e.g.: 1024000 which is in bytes.
            - Input may be alpha-numeric, e.g.: 1M, 1G, 1T.
        required: false
        type: str
    refreservation:
        description: Guaranteed amount of space to reserve for this dataset, not including its descendents.
        required: false
        type: str
    reservation:
        description: Guaranteed amount of space to reserve for this dataset a nd descendent datasets.
        required: false
        type: str
    nbmand:
        description: Non-blocking mandatory locking. This setting may may to dataset sharing.
        required: false
        type: str
    state:
        description: Specifies expected state of the share, i.e. whether or not it should be present.
        required: false
        type: str

# Specify this value according to your collection
# in format of namespace.collection.doc_fragment_name
extends_documentation_fragment:
    - my_namespace.my_collection.my_doc_fragment_name

author:
    - Sam Zaydel (sz@racktopsystems.com)
"""


def main():
    # define available arguments/parameters a user can pass to the module
    module_args = dict(
        host=dict(type="str", required=False, default="localhost"),
        port=dict(type="int", required=False, default=8443),
        username=dict(type="str", required=False, default="root"),
        password=dict(
            type="str",
            required=True,
            no_log=True,
            fallback=(env_fallback, ["ANSIBLE_BRICKSTOR_PASSWORD"]),
        ),
        ds_path=dict(type="str", required=True),
        # The following applies to destroys
        recursive=dict(type="bool", required=False, default=False),
        state=dict(
            type="str", required=False, default="present", choices=["absent", "present"]
        ),
        # The following dataset properties could be influenced by the user.
        # FIXME: Should have choices here for storage_profile beyond custom and
        # default.
        storage_profile=dict(
            type="str",
            required=False,
            choices=["general_filesystem", "custom_filesystem", "vmware_filesystem"],
            default="general_filesystem",
        ),
        # ub=dict(type="bool", required=False, default=True),
        aclmode=dict(
            type="str",
            default="passthrough",
            choices=["discard" "groupmask", "passthrough", "restricted"],
        ),
        aclinherit=dict(
            type="str",
            default="passthrough",
            choices=["discard" "groupmask", "passthrough", "restricted"],
        ),
        atime=dict(type="str", default="on", choices=["on", "off"]),
        quota=dict(type="str", required=False, default=None),
        refquota=dict(type="str", required=False, default=None),
        reservation=dict(type="str", required=False, default=None),
        refreservation=dict(type="str", required=False, default=None),
        nbmand=dict(type="str", default="on", choices=["on", "off"]),
    )

    # seed the result dict in the object
    # we primarily care about changed and state
    # changed is if this module effectively modified the target
    # state will include any data that you want your module to pass back
    # for consumption, for example, in a subsequent task
    result = dict(changed=False, comment="")

    # the AnsibleModule object will be our abstraction working with Ansible
    # this includes instantiation, a couple of common attr would be the
    # args/params passed to the execution, as well as if the module
    # supports check mode
    module = AnsibleModule(argument_spec=module_args, supports_check_mode=True)

    # if the user is working with this module in only check mode we do not
    # want to make any changes to the environment, just return the current
    # state with no modifications
    if module.check_mode:
        module.exit_json(**result)

    params = module.params
    ds_path = params["ds_path"]
    result["comment"] = "No changes were made"

    c = api.AnsibleBsrApiClient(
        api.ApiCreds(u=module.params["username"], p=module.params["password"]),
        host=module.params["host"],
        port=module.params["port"],
    )
    ds = api_datasets.Dataset()

    if params["state"] == "present":
        props = {
            "aclmode": params["aclmode"],
            "aclinherit": params["aclinherit"],
            "atime": params["atime"],
            "quota": params["quota"],
            "refquota": params["refquota"],
            "reservation": params["reservation"],
            "refreservation": params["refreservation"],
            "nbmand": params["nbmand"],
            "racktop:storage_profile": params["storage_profile"],
            # "racktop:ub": True,
        }

        # If the dataset already exists, the underlying API is going to attempt
        # to update properties based on those supplied. The API will tell is
        # whether or not any actual modifications occurred. This is important,
        # because we need to report whether or not there was a state change.
        resp = ds.create_dataset(ds_path, c, **props)
        result["changed"] = resp.changed

        msg = f"Failed to create or update dataset {ds_path}"
        if resp.succeeded:
            outcome = resp.details["outcome"]
            if outcome == "created":
                msg = f"Created dataset {ds_path}"
            elif outcome == "modified":
                msg = f"Modified properties on dataset {ds_path}"
                result["details"] = {"updates": resp.details["updates"]}
            elif outcome == "unchanged":
                msg = f"No changes made to dataset {ds_path}"
            result["comment"] = msg
        else:
            if resp.error != "":
                msg += f" | {resp.error}"
            module.fail_json(msg, **result)

    elif params["state"] == "absent":
        msg = f"Failed to destroy dataset {ds_path}"
        resp = ds.destroy_dataset(ds_path, c, recursive=True)
        result["changed"] = resp.changed
        if resp.succeeded:
            if resp.changed:
                msg = f"Destroyed dataset {ds_path}"
            else:
                msg = f"No changes to already absent dataset {ds_path}"
            result["comment"] = msg
        else:
            module.fail_json(msg, **result)

    if False:
        module.fail_json(msg="This is a bug; must not be possible", **result)

    # in the event of a successful module execution, you will want to
    # simple AnsibleModule.exit_json(), passing the key/value results
    module.exit_json(**result)


if __name__ == "__main__":
    main()
