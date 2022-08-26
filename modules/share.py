#!/usr/bin/env python3
__metaclass__ = type
from dataclasses import dataclass
from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.basic import env_fallback

if True:
    from ..module_utils import api_datasets
    from ..module_utils import api
    from ..module_utils import netacl
    from ..module_utils import smb

from ansible.module_utils import api
from ansible.module_utils import api_datasets
from ansible.module_utils import smb


# See https://sourcegraph.com/github.com/illumos/illumos-gate@master/-/blob/usr/src/uts/common/smbsrv/smb_sid.h for additional details.
# define NT_BUILTIN_CURRENT_OWNER_SIDSTR         "S-1-5-32-766"
# define NT_BUILTIN_CURRENT_GROUP_SIDSTR         "S-1-5-32-767"


DOCUMENTATION = r"""
---
module: share

short_description: Enables management of shares

# If this is part of a collection, you need to use semantic versioning,
# i.e. the version is of the form "2.5.0" and not "2.4".
version_added: "1.0.0"

description: This module enables configuration of NFS and SMB shares and perms
on datasets. This module operates on already-existing datasets and tasks are
going to fail if the path to a dataset does not actually exist. Certain settings
apply to both NFS and SMB shares, however some only apply to one or the other.


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
    filesystem_acls:
        description: Filesystem Access Control List. Every filesystem has a
        default ACL out of the box, which is most likely unsuitable and this
        replaces it with the user-specified ACL.
        required: true
        type: list
    owner_sid:
        description: SID of the user acting as the owner of the dataset. This
        ownership will be inherited by child objects.
        required: true
        type: str
    owner_group_sid:
        description: Group SID of the user acting as the owner of the dataset.
        This ownership will be inherited by child objects.
        required: true
        type: str
    proto:
        description: Protocol to which these settings apply, either SMB or NFS.
        required: true
        type: str
    share_name:
        description: Name to assign to this share, only relevant if sharing
        over SMB.
        required: false
        type: str
    rw_access_list:
        description: Network Access Control List specifying who has read/write 
        access to the share.
        required: false
        type: list
    ro_access_list:
        description: Network Access Control List specifying who has read-only
        access to the share.
        required: false
        type: list
    no_access_list:
        description: Network Access Control List specifying who is denied
        access to the share.
        required: false
        type: list
    abe:
        description: Access Based Enumeration limits what directories a user can see to those which they can access.
        required: false
        type: bool
    csc:
        description: Client Side Caching policy. This controls what if any
        caching clients can do. Be careful with this setting. It may render
        User Behaviour and Active Defense useless.
        required: false
        type: str
    encrypt:
        description: Controls protocol-level encryption requirements.
        Applicable only to SMB shares for the moment.
        required: false
        type: str
    ub:
        description: Controls user behavior monitoring on the dataset.
        required: false
        type: bool
    state:
        description: Specifies expected state of the share, i.e. whether or not 
        it should be present.
        required: false
        type: str

# Specify this value according to your collection
# in format of namespace.collection.doc_fragment_name
extends_documentation_fragment:
    - my_namespace.my_collection.my_doc_fragment_name

author:
    - Sam Zaydel (sz@racktopsystems.com)
"""

from ansible.module_utils.basic import AnsibleModule


def share_is_absent(module, result):
    params = module.params
    c = api.AnsibleBsrApiClient(
        api.ApiCreds(u=params["username"], p=params["password"]),
        host=module.params["host"],
        port=module.params["port"],
    )

    ds_path = params["ds_path"]
    ub = params["ub"]
    proto = params["proto"]
    ds = api_datasets.Dataset()
    if proto == "nfs":
        pass  # Need to implement NFS

    if proto == "smb":
        msg = f"Failed to disable SMB share on {ds_path}"
        ds = api_datasets.Dataset({"sharesmb": "off"})
        resp = ds.configure_smb_share(ds_path, c)
        if resp.error != "":
            msg += f" | {resp.error}"
            module.fail_json(msg, **result)
        if resp.changed:
            result["changed"] = True
            result["details"].update(
                {
                    "smb": resp.details,
                    "racktop:ub": ub,
                }
            )
            result["comment"].append(f"disabled SMB share on {ds_path}")


def share_is_present(module, result):
    params = module.params
    c = api.AnsibleBsrApiClient(
        api.ApiCreds(u=params["username"], p=params["password"]),
        host=module.params["host"],
        port=module.params["port"],
    )

    ds_path = params["ds_path"]
    share_name = None
    if params["share_name"] == "":
        share_name = ds_path.split("/")[-1]
    else:
        share_name = params["share_name"]
    ro_access_list = params["ro_access_list"]
    rw_access_list = params["rw_access_list"]
    no_access_list = params["no_access_list"]
    abe = params["abe"]
    csc = params["csc"]
    encrypt = params["encrypt"]
    ub = params["ub"]
    proto = params["proto"]
    owner_sid = params["owner_sid"]
    owner_group_sid = params["owner_group_sid"]
    filesystem_acls = params["filesystem_acls"]
    ds = api_datasets.Dataset()
    # We first want to make sure that ACLs on the filesystem are setup and
    # if this fails, then we should stop processing this task.
    resp = ds.set_permissions(ds_path, filesystem_acls, owner_sid, owner_group_sid, c)
    if resp.error != "":
        msg = f"Failed to set ACLs on {ds_path} | {resp.error}"
        module.fail_json(msg, **result)

    if resp.changed:
        result["changed"] = True
        result["comment"].append("ACL on the filesystem was modified")
        result["details"].update(
            {
                "filesystem_acl": resp.details,
            }
        )
    else:  # No change was necessary
        result["comment"].append("ACL on the filesystem already matched desired state")

    if proto == "nfs":
        pass  # Need to implement NFS

    if proto == "smb":
        msg = f"Failed to enable SMB share on {ds_path}"
        # We have to support NFS and SMB here. For the moment it is only SMB.
        share = smb.SMBShare(
            share_name,
            abe,
            csc,
            encrypt,
            ro_access_list,
            rw_access_list,
            no_access_list,
            ub,
        )
        try:
            share.validate_access_lists()
            ds = api_datasets.Dataset(share.property_pairs)
            # resp = ds.configure_smb_share(ds_path, c, **share.property_pairs)
            resp = ds.configure_smb_share(ds_path, c)
            if resp.error != "":
                msg += f" | {resp.error}"
                module.fail_json(msg, **result)
            if resp.changed:
                result["changed"] = True
                result["details"].update({"smb": resp.details})
                result["comment"].append(f"configured SMB share on {ds_path}")

        except netacl.InvalidAddressSpecification as e:
            msg += f" | {e.args[0]}"
            module.fail_json(msg, **result)
        except ValueError as e:
            msg += f" | {e.args[0]}"
            module.fail_json(msg, **result)


def run_module():
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
        filesystem_acls=dict(type="list", required=False, default=[]),
        owner_sid=dict(type="str", required=False, default=""),
        owner_group_sid=dict(type="str", required=False, default=""),
        proto=dict(type="str", required=True, choices=["nfs", "smb"]),
        share_name=dict(type="str", required=False, default=""),
        rw_access_list=dict(type="list", required=False, default=""),
        ro_access_list=dict(type="list", required=False, default=""),
        no_access_list=dict(type="list", required=False, default=""),
        abe=dict(type="bool", required=False, default=False),
        csc=dict(
            type="str",
            required=False,
            default="disabled",
            choices=["auto", "disabled", "manual", "vdo"],
        ),
        encrypt=dict(
            type="str",
            required=False,
            default="required",
            choices=["disabled", "enabled", "required"],
        ),
        ub=dict(type="bool", required=False, default=True),
        state=dict(
            type="str", required=False, default="present", choices=["absent", "present"]
        ),
        # recursive=dict(type="bool", required=False, default=False),
    )

    result = dict(changed=False, comment="")

    required_if = [
        (
            "state",
            "present",
            ("ds_path", "filesystem_acls", "owner_sid", "owner_group_sid", "proto"),
        ),
        ("state", "absent", ("ds_path",)),
    ]
    module = AnsibleModule(
        argument_spec=module_args, required_if=required_if, supports_check_mode=True
    )

    # if the user is working with this module in only check mode we do not
    # want to make any changes to the environment, just return the current
    # state with no modifications
    if module.check_mode:
        module.exit_json(**result)

    state = module.params["state"]

    result["changed"] = False
    result["comment"] = []
    result["details"] = dict()

    if state == "present":
        share_is_present(module, result)
    else:  # share is absent
        share_is_absent(module, result)

    module.exit_json(**result)


def main():
    run_module()


if __name__ == "__main__":
    main()
