from dataclasses import dataclass
import json
from typing import List, Dict

from pprint import pprint

from enum import Enum

import requests

from contextlib import contextmanager


ZFS_DATASET_ENDPOINT = "/internal/v1/zfs/dataset"
ZFS_DATASET_DESTROY_ENDPOINT = "/internal/v1/dataset/destroy"
ZFS_DATASET_PERMS_ENDPOINT = "/internal/v1/fs/perms"
ZFS_DATASET_PERMS_APPLY_ENDPOINT = "/internal/v1/fs/perms/apply"
SHELL_RUN_ENDPOINT = "/internal/v1/shell/run"

ZFS_CMD = "/usr/sbin/zfs"
BSRZFS_CMD = "/usr/racktop/sbin/bsrzfs"
API_TIMEOUT = 60

# READ_ONLY_PROPS = (
#     "casesensitivity",
#     "compressratio",
#     "creation",
#     "createtxg",
#     "filesystem_count",
#     "guid",
#     "mounted",
#     "normalization",
#     "refcompressratio",
#     "type",
#     "usedbychildren",
#     "usedbyrefreservation",
#     "snapshot_count",
#     "utf8only",
#     "encryption",
#     "keyformat",
#     "pbkdf2iters",
#     "special_small_blocks",
# )

KNOWN_PROPS = (
    "aclinherit",
    "aclmode",
    "atime",
    "canmount",
    "checksum",
    "compression",
    "copies",
    "devices",
    "exec",
    "filesystem_limit",
    "logbias",
    "nbmand",
    "casesensitivity",
    "normalization",
    "utf8only",
    "primarycache",
    "quota",
    "readonly",
    "recordsize",
    "redundant_metadata",
    "refquota",
    "refreservation",
    "reservation",
    "secondarycache",
    "setuid",
    "snapdir",
    "snapshot_limit",
    "sync",
    "vscan",
    "xattr",
    "zoned",
    "racktop:storage_profile",
    "racktop:encoded_description",
    "smartfolders",
    "racktop:ub",
    "racktop:ub_suspend",
    "racktop:ub_thresholds",
    "racktop:ub_trial",
    "racktop:version",
    "sharenfs",
    "sharesmb",
)


class BsrApiPermsRespose:
    def __init__(self, resp_obj: Dict, failed=False):
        self._data = resp_obj.get("Data")
        self._perms = resp_obj.get("Permissions")
        self._task = resp_obj.get("Task")
        self._acl = None
        self._failed = failed
        self._owner_group_sid = None
        self._owner_sid = None
        self._dataset_id = None
        self._error_code = 0
        if not self._failed:
            # Permissions is an array in the response object, but it looks like
            # we get only one item in the array when we are dealing with a
            # single dataset. This may need to be improved later, but for now
            # this seems to be sufficient.
            if self._perms:
                self._acl = self._perms[0].get("Acl")
                self._dataset_id = self._perms[0].get("DatasetId")
                self._owner_group_sid = self._perms[0].get("OwnerGroupSid")
                self._owner_sid = self._perms[0].get("OwnerSid")
            elif self._task:
                self._acl = self._task.get("Acl")
                self._dataset_id = self._task.get("DatasetId")
                self._owner_group_sid = self._task.get("OwnerGroupSid")
                self._owner_sid = self._task.get("OwnerSid")
        else:
            self._error_code = self._data["Code"]
            if "Dataset not found" in self._data["Message"]:
                self._error_code = DatasetErrors.DoesNotExist

    @property
    def failed(self):
        return self._failed

    @property
    def error_code(self):
        return self._error_code

    @property
    def error_message(self):
        if self._failed:
            return self._data.get("Message")

    @property
    def error_type(self):
        if self._failed:
            return self._data.get("ErrType")
        return None

    @property
    def permissions(self):
        return self._perms[0]

    @property
    def dataset_id(self) -> str:
        return self._dataset_id

    @property
    def owner_group_sid(self) -> str:
        return self._owner_group_sid

    @property
    def owner_sid(self) -> str:
        return self._owner_sid

    @property
    def acl(self) -> List[Dict[str, str]]:
        return self._acl

    def acl_iter(self):
        for ace in self._acl:
            yield ace


class BsrApiCommandResponse:
    def __init__(self, resp_obj):
        self.result: dict = resp_obj.get("Result", {})
        if not self.result:
            raise EmptyRespObject("Cannot handle an empty API response")
        self._error_code = self.result.get("ExitCode", "-1")
        self._stdout = self.result.get("StdOut")
        self._stderr = self.result.get("StdErr")

    @property
    def failed(self):
        return self._error_code != 0

    @property
    def error_code(self):
        return self._error_code

    @property
    def stdout(self):
        return self._stdout

    @property
    def stderr(self):
        return self._stderr


class DatasetErrors(Enum):
    ConnectionTimeout = 1000
    DoesNotExist = 1001
    Exists = 1002
    RequiresRecursiveDestroy = 1020
    DoesNotHaveEnoughSpace = 1030


@contextmanager
def suppress_insecure_https_warnings():
    import warnings
    import urllib3

    with warnings.catch_warnings():
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        yield None


@dataclass
class ApiCreds:
    u: str
    p: str

    @property
    def user(self):
        return self.u

    @property
    def passwd(self):
        return self.p


class LoginError(Exception):
    pass


class DatasetQueryError(Exception):
    def __init__(self, error_code: int, error_type: str, *args: object) -> None:
        self.error_code = error_code
        self.error_type = error_type
        super().__init__(*args)


class EmptyRespObject(Exception):
    pass


class AnsibleBsrApiClient:
    def __init__(
        self,
        cr: ApiCreds,
        host="localhost",
        port=8443,
        timeout=API_TIMEOUT,
        verify=False,
    ) -> None:
        self.cr = cr
        self.host = host
        self.port = port
        self.token = None
        self.timeout = timeout
        self.verify = verify
        self.api_conn_err = None

    def url(self, route):
        return f"https://{self.host}:{self.port}/{route.lstrip('/')}"

    @property
    def _headers(self):
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "User-Agent": "BsrCli",  # This is why we cannot have nice things :)
        }

    def _login(self, username: str, passwd: str):
        with suppress_insecure_https_warnings():
            try:
                resp = requests.post(
                    self.url("/login"),
                    auth=(username, passwd),
                    verify=self.verify,
                    timeout=self.timeout,
                )
                if resp.status_code == 200:
                    token_obj = resp.json()
                    if not token_obj["token"]:
                        raise LoginError("Token value cannot be an empty string")
                    return token_obj["token"]
                raise LoginError(resp.content.__str__())
            # This will happen if we cannot connect to the API, which may or
            # may not be bsrapid.
            except requests.exceptions.ConnectTimeout as err:
                self.api_conn_err = err
                return None

    def login(self):
        self.token = self._login(self.cr.user, self.cr.passwd)

    def auth_if_required(self):
        # There should eventually be a check here to make sure that token is
        # not expired. We may have our token, but it may not be usable any
        # longer.
        if not self.token:
            self.login()
            if self.api_conn_err is not None:
                raise DatasetQueryError(
                    DatasetErrors.ConnectionTimeout,
                    "API Connection Error",
                    self.api_conn_err.args[0],
                )
        return True

    # def post_shell_command(
    #     self, cmd: str, args: str = "", use_shell=True, is_query=True, requests=requests
    # ) -> Dict:
    #     """Sends a command POST request to API. This may or may not be a
    #     mutating command.

    #     Args:
    #         cmd (str): Command to execute on the appliance.
    #         args (str, optional): Arguments which the command will accept. Defaults to "".
    #         requests (_type_, optional): Allows injection of custom requests implementation. Defaults to requests.

    #     Returns:
    #         dict: Response object from the API call, converted into a dict.
    #     """
    #     self.auth_if_required()

    #     cmd_data = {
    #         "Command": cmd,
    #         "Args": args,
    #         "UseShell": use_shell,
    #         "IsQuery": is_query,
    #         "OperationOptions": {"ClientTxId": ""},
    #     }

    #     with suppress_insecure_https_warnings():
    #         resp = requests.post(
    #             self.url(SHELL_RUN_ENDPOINT),
    #             headers=self._headers,
    #             data=json.dumps(cmd_data),
    #             verify=self.verify,
    #         )
    #     return resp.json()

    def _parse_dataset_details(self, props_str: str):
        props = dict()
        # API gives us a single string that we have to manipulate into a native
        # data structure. We parse the string, splitting it into a list of lines and
        # then handle each line which is a single prop.
        lines = props_str.split("\n")
        for line in lines:
            _, prop, value, _ = line.split("\t")
            if prop not in KNOWN_PROPS:
                continue
            elif value == "none":
                props[prop] = None
            elif value.isnumeric():
                # FIXME: This needs to be checked in terms of speed. It seems
                # like it may be slower to check membership.
                if prop in (
                    "quota",
                    "refquota",
                    "reservation",
                    "refreservation",
                ):
                    if value == "0":
                        props[prop] = None
                    else:
                        props[prop] = value
                else:
                    props[prop] = int(value)
            else:
                props[prop] = value
        return props

    def get_dataset_properties(self, ds_path: str):
        cmd = {
            "Command": ZFS_CMD,
            "Args": f"get -Hp -t filesystem,volume all {ds_path}",
        }
        with suppress_insecure_https_warnings():
            self.auth_if_required()
            resp = requests.post(
                self.url(SHELL_RUN_ENDPOINT),
                headers=self._headers,
                data=json.dumps(cmd),
                verify=self.verify,
                timeout=self.timeout,
            )
            result = BsrApiCommandResponse(resp.json())
            if result.failed:
                if "dataset does not exist" in result.stderr:
                    raise DatasetQueryError(
                        DatasetErrors.DoesNotExist, None, result.stderr
                    )
                raise DatasetQueryError(result.error_code, None, result.stderr)
        return self._parse_dataset_details(result.stdout)

    def _raise_command_failure(self, result: BsrApiCommandResponse):
        if result.failed:
            if (
                "size is greater than available space" in result.stderr
                or "out of space" in result.stderr
            ):
                raise DatasetQueryError(
                    DatasetErrors.DoesNotHaveEnoughSpace, None, result.stderr
                )
            elif "dataset already exists" in result.stderr:
                raise DatasetQueryError(DatasetErrors.Exists, None, result.stderr)
            raise DatasetQueryError(result.error_code, None, result.stderr)
        return result

    def create_dataset(self, ds_path: str, requests=requests, **props):
        args = f'create {" ".join([f"-o {k}={v}" for k, v in props.items()])} {ds_path}'

        data = {
            "Command": BSRZFS_CMD,
            "Args": args,
            "UseShell": False,
            "IsQuery": False,
        }
        with suppress_insecure_https_warnings():
            self.auth_if_required()
            resp = requests.post(
                self.url(SHELL_RUN_ENDPOINT),
                headers=self._headers,
                data=json.dumps(data),
                verify=self.verify,
                timeout=self.timeout,
            )
            result = BsrApiCommandResponse(resp.json())
            return self._raise_command_failure(result)

    def set_dataset_properties(
        self, ds_path: str, requests=requests, **props
    ) -> BsrApiCommandResponse:
        """Sets dataset properties.

        Args:
            ds_path (str): Properties are set on this dataset.
            requests (_type_, optional): Allows injection of custom requests implementation. Defaults to requests.

        Raises:
            DatasetQueryError: An exception with some information about what failed.
            ValueError: Raised when props is an empty dictionary.

        Returns:
            BsrApiCommandResponse: Response from the API converted into a native type.
        """
        if not props:
            raise ValueError("properties argument cannot be an empty dictionary")
        args = f'set {" ".join([f"{k}={v}" for k, v in props.items()])} {ds_path}'

        data = {
            "Command": ZFS_CMD,
            "Args": args,
            "UseShell": False,
            "IsQuery": False,
        }
        with suppress_insecure_https_warnings():
            self.auth_if_required()
            resp = requests.post(
                self.url(SHELL_RUN_ENDPOINT),
                headers=self._headers,
                data=json.dumps(data),
                verify=self.verify,
                timeout=self.timeout,
            )
            result = BsrApiCommandResponse(resp.json())
            self._raise_command_failure(result)
            return result

    def get_dataset_perms(self, ds_path: str, requests=requests) -> BsrApiPermsRespose:
        with suppress_insecure_https_warnings():
            self.auth_if_required()
            resp = requests.get(
                self.url(ZFS_DATASET_PERMS_ENDPOINT),
                headers=self._headers,
                params={
                    "dataset": ds_path,
                    "recursive": True,
                    "resolve_identities": True,
                },
                verify=self.verify,
                timeout=self.timeout,
            )
            result = BsrApiPermsRespose(resp.json(), failed=resp.status_code != 200)
            if result.failed:
                # Dataset not existing is a common scenario, but we
                # unfortunately do not get a 404 in this scenario. We get a 500
                # instead. A bit of tweaking happens in the __init__ of
                # BsrApiPermsResponse letting us alter our message in the case
                # this is a well known failure mode.
                if result.error_code == DatasetErrors.DoesNotExist:
                    raise DatasetQueryError(
                        result.error_code,
                        result.error_type,
                        f"unable to obtain ACL because dataset {ds_path} does not appear to exist",
                    )
                raise DatasetQueryError(
                    result.error_code, result.error_type, result.error_message
                )
            return result

    def set_dataset_perms(
        self,
        ds_id: str,
        acl: List[Dict[str, str]],
        owner_sid: str,
        owner_group_sid: str,
        recursive=False,
        requests=requests,
    ) -> BsrApiPermsRespose:
        """Modifies ACL and sets User SID and Group SID on the given dataset.

        Args:
            ds_id (str): Dataset ID instead of the usual dataset path.
            acl (List[Dict[str, str]]): List of ACEs that should be applied to the dataset.
            owner_sid (str): SID of the user who owns this dataset.
            owner_group_sid (str): Group SID of the group owner of this dataset.
            recursive (bool, optional): Whether or not this change should be applied recursively. Defaults to False.
            requests (_type_, optional): Allows injection of custom requests implementation. Defaults to requests.

        Raises:
            DatasetQueryError: An exception with some information about what failed.

        Returns:
            BsrApiPermsRespose: Response from the API converted into a native type.
        """
        # First, we need to convert the name of the dataset to an ID, which we
        # will use in a subsequent call to apply settings to the filesystem.
        # We do not bother trying to auth here, because it will happen in the
        # get_dataset_perms method call. We do not bother handling an exception
        # which may be raised here and let it bubble up to the caller. This
        # interface is meant for consumption outside of this class and by
        # extension exceptions raised.
        data = {
            "DatasetId": ds_id,
            "Recursive": recursive,
            "SingleDatasetOnly": False,  # I still don't fully grok this one
            "Acl": acl,
            "OwnerSid": owner_sid,
            "OwnerGroupSid": owner_group_sid,
            "WaitUntilComplete": False,
            "ClientTxId": None,
        }
        with suppress_insecure_https_warnings():
            self.auth_if_required()
            resp = requests.post(
                self.url(ZFS_DATASET_PERMS_APPLY_ENDPOINT),
                headers=self._headers,
                data=json.dumps(data),
                verify=self.verify,
                timeout=self.timeout,
            )
            result = BsrApiPermsRespose(resp.json())
            if result.failed:
                raise DatasetQueryError(result.error_code, None, result.stderr)
            return result

    def share_dataset(
        self, ds_path: str, nfs_opts=None, smb_opts=None, requests=requests
    ):
        pairs = []
        if nfs_opts:
            if " " in nfs_opts:
                raise ValueError(
                    "share settings must not contain whitespace characters"
                )
            pairs.append(("sharenfs", nfs_opts))
        if smb_opts:
            if " " in smb_opts:
                raise ValueError(
                    "share settings must not contain whitespace characters"
                )
            pairs.append(("sharesmb", smb_opts))

        props = {k: v for k, v in pairs}
        args = f'set {" ".join([f"{k}={v}" for k, v in props.items()])} {ds_path}'

        data = {
            "Command": ZFS_CMD,
            "Args": args,
            "UseShell": False,
            "IsQuery": False,
        }
        with suppress_insecure_https_warnings():
            self.auth_if_required()
            resp = requests.post(
                self.url(SHELL_RUN_ENDPOINT),
                headers=self._headers,
                data=json.dumps(data),
                verify=self.verify,
                timeout=self.timeout,
            )
            result = BsrApiCommandResponse(resp.json())
            if result.failed:
                raise DatasetQueryError(result.error_code, None, result.stderr)
            return result

    def unshare_dataset(
        self, ds_path: str, disable_nfs=True, disable_smb=True, requests=requests
    ) -> BsrApiCommandResponse:
        """Disable sharing via NFS or SMB or both on a given dataset.

        Args:
            ds_path (str): Path to dataset to be destroyed.
            disable_nfs (bool, optional): Disables NFS share if True. Defaults to True.
            disable_smb (bool, optional): Disables SMB share if true. Defaults to True.
            requests (_type_, optional): Allows injection of custom requests implementation. Defaults to requests.

        Raises:
            DatasetQueryError: An exception with some information about what failed.

        Returns:
            BsrApiCommandResponse: Response from the API converted into a native type.
        """
        pairs = []
        if disable_nfs:
            pairs.append(("sharenfs", "off"))
        if disable_smb:
            pairs.append(("sharesmb", "off"))
        props = {k: v for k, v in pairs}
        args = f'set {" ".join([f"{k}={v}" for k, v in props.items()])} {ds_path}'

        data = {
            "Command": ZFS_CMD,
            "Args": args,
            "UseShell": False,
            "IsQuery": False,
        }
        with suppress_insecure_https_warnings():
            self.auth_if_required()
            resp = requests.post(
                self.url(SHELL_RUN_ENDPOINT),
                headers=self._headers,
                data=json.dumps(data),
                verify=self.verify,
                timeout=self.timeout,
            )
            result = BsrApiCommandResponse(resp.json())
            if result.failed:
                raise DatasetQueryError(result.error_code, None, result.stderr)
            return result

    def destroy_dataset(self, ds_path: str, recursive=False) -> bool:
        """Destroys datasets, potentially recursively, if the recursive flag is set.

        Args:
            ds_path (str): Path to dataset to be destroyed.
            recursive (bool, optional): Recurse through children. Defaults to False.

        Raises:
            DatasetQueryError: An exception with some information about what failed.

        Returns:
            bool: True if operation succeeded, False otherwise.
        """
        data = {"Dataset": ds_path}
        with suppress_insecure_https_warnings():
            self.auth_if_required()
            resp = requests.post(
                self.url(ZFS_DATASET_DESTROY_ENDPOINT),
                headers=self._headers,
                data=json.dumps(data),
                verify=self.verify,
                timeout=self.timeout,
            )
            # It is not straight-forward to tell here whether we actually
            # succeeded or failed, because the API returns 200 and we may fail
            # for a number of reasons, such as destroying a dataset with
            # children. We need to inspect the payload to figure out what state
            # we are actually in.
            if resp.status_code != 200:
                resp.raise_for_status()

            # If there are no descendant datasets and there is no error,
            # operation succeeded. We will succeed even if we destroy a dataset
            # which does not exist.
            resp_dict = resp.json()
            if not resp_dict["Descendants"] and not resp_dict["Error"]:
                return True

            # If there are descendants and we do not have the recursive flag
            # set, we are going to raise an exception at this point.
            if resp_dict["Descendants"] and not recursive:
                raise DatasetQueryError(
                    DatasetErrors.RequiresRecursiveDestroy,
                    "",
                    resp_dict["Error"],
                    resp_dict["Descendants"],
                )
            elif (
                resp_dict["Descendants"] and recursive
            ):  # Recurse through child datasets here.
                children_unsorted = [c["Path"] for c in resp_dict["Descendants"]]
                children_sorted = reversed(sorted(children_unsorted))
                for child_ds in children_sorted:
                    self.destroy_dataset(child_ds, recursive=True)
                # Finally destroy the parent dataset.
                self.destroy_dataset(ds_path)
            return True

    def is_existing_dataset(self, ds_path: str):
        with suppress_insecure_https_warnings():
            self.auth_if_required()
            resp = requests.get(
                self.url(ZFS_DATASET_ENDPOINT),
                headers=self._headers,
                params={"dataset": ds_path},
                verify=self.verify,
                timeout=self.timeout,
            )
            if resp.status_code == 200:
                return True
            resp_dict = resp.json()
            if resp.status_code == 500:
                if resp_dict["Data"].get("Message", "") == "No such dataset.":
                    return False
            # FIXME: This is temporary, needs to be improved. The caller should
            # not have to deal with errors from the http library.
            resp.raise_for_status()
