from dataclasses import dataclass
import json
from sys import stderr
from typing import Dict
from urllib.parse import urlencode

from enum import Enum

import requests

from contextlib import contextmanager


ZFS_DATASET_ENDPOINT = "/internal/v1/zfs/dataset"
ZFS_DATASET_DESTROY_ENDPOINT = "/internal/v1/dataset/destroy"
SHELL_RUN_ENDPOINT = "/internal/v1/shell/run"
ZFS_CMD = "/usr/sbin/zfs"
BSRZFS_CMD = "/usr/racktop/sbin/bsrzfs"

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


class BsrApiCommandResponse:
    def __init__(self, resp_obj):
        self.result: dict = resp_obj.get("Result", {})
        if not self.result:
            raise EmptyRespObject("Cannot handle an empty API response")
        self._exit_code = self.result.get("ExitCode", "-1")
        self._stdout = self.result.get("StdOut")
        self._stderr = self.result.get("StdErr")

    @property
    def command_failed(self):
        return self._exit_code != 0

    @property
    def exit_code(self):
        return self._exit_code

    @property
    def stdout(self):
        return self._stdout

    @property
    def stderr(self):
        return self._stderr


class DatasetErrors(Enum):
    DoesNotExist = 1000
    Exists = 1001
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
        verify=False,
    ) -> None:
        self.cr = cr
        self.host = host
        self.port = port
        self.token = None
        self.verify = verify

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
            resp = requests.post(
                self.url("/login"), auth=(username, passwd), verify=self.verify
            )
            if resp.status_code == 200:
                token_obj = resp.json()
                if not token_obj["token"]:
                    raise LoginError("Token value cannot be an empty string")
                return token_obj["token"]
            raise LoginError(resp.content.__str__())

    def login(self):
        self.token = self._login(self.cr.user, self.cr.passwd)

    def auth_if_required(self):
        # There should eventually be a check here to make sure that token is
        # not expired. We may have our token, but it may not be usable any
        # longer.
        if not self.token:
            # We don't bother checking for error here. If there is a problem
            # with login, an exception will be surfaced below.
            self.login()

    def post_shell_command(
        self, cmd: str, args: str = "", use_shell=True, is_query=True
    ) -> Dict:
        """Sends a command POST request to API. This may or may not be a
        mutating command.

        Args:
            cmd (str): Command to execute on the appliance.
            args (str, optional): Arguments which the command will accept. Defaults to "".

        Returns:
            dict: Response object from the API call, converted into a dict.
        """
        self.auth_if_required()

        cmd_data = {
            "Command": cmd,
            "Args": args,
            "UseShell": use_shell,
            "IsQuery": is_query,
            "OperationOptions": {"ClientTxId": ""},
        }

        with suppress_insecure_https_warnings():
            resp = requests.post(
                self.url(SHELL_RUN_ENDPOINT),
                headers=self._headers,
                data=json.dumps(cmd_data),
                verify=self.verify,
            )
        return resp.json()

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
            )
            result = BsrApiCommandResponse(resp.json())
            if result.command_failed:
                if "dataset does not exist" in result.stderr:
                    raise DatasetQueryError(
                        DatasetErrors.DoesNotExist, None, result.stderr
                    )
                raise DatasetQueryError(result.exit_code, None, result.stderr)
        return self._parse_dataset_details(result.stdout)

    def _raise_with_details(self, result: BsrApiCommandResponse):
        if result.command_failed:
            if "size is greater than available space" in result.stderr:
                raise DatasetQueryError(
                    DatasetErrors.DoesNotHaveEnoughSpace, None, result.stderr
                )
            elif "dataset already exists" in result.stderr:
                raise DatasetQueryError(
                    DatasetErrors.Exists, None, result.stderr
                )
            raise DatasetQueryError(result.exit_code, None, result.stderr)
        return


    def create_dataset(self, ds_path: str, requests=requests, **props):
        args = f'create {" ".join([f"-o {k}={v}" for k, v in props.items()])} {ds_path}'

        data = {
            "Command": "/usr/racktop/sbin/bsrzfs",
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
            )
            result = BsrApiCommandResponse(resp.json())
            # if result.command_failed:
            #     if "dataset already exists" in result.stderr:
            #         raise DatasetQueryError(
            #             DatasetErrors.Exists, None, result.stderr
            #         )
            #     elif "size is greater than available space" in result.stderr:
            #         raise DatasetQueryError(
            #             DatasetErrors.DoesNotHaveEnoughSpace, None, result.stderr
            #         )
            #     raise DatasetQueryError(result.exit_code, None, result.stderr)
            self._raise_with_details(result)
            return result

    def set_dataset_properties(self, ds_path: str, requests=requests, **props):
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
            )
            result = BsrApiCommandResponse(resp.json())
            # if result.command_failed:
            #     raise DatasetQueryError(result.exit_code, None, result.stderr)
            # return result
            self._raise_with_details(result)
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
            )
            result = BsrApiCommandResponse(resp.json())
            if result.command_failed:
                raise DatasetQueryError(result.exit_code, None, result.stderr)
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
            )
            result = BsrApiCommandResponse(resp.json())
            if result.command_failed:
                raise DatasetQueryError(result.exit_code, None, result.stderr)
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
