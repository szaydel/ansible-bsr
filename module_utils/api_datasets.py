from copy import deepcopy
from dataclasses import dataclass
from typing import Dict, List

from .api import AnsibleBsrApiClient
from .api import DatasetErrors
from .api import DatasetQueryError
from .api import KNOWN_PROPS


@dataclass
class DatasetTaskResult:
    """
    An object normally returned to consumer of methods on the Dataset class.
    """

    succeeded: bool
    changed: bool
    error: str
    details: Dict


IGNORED_PROPS = (
    # "available",
    # "logicalreferenced",
    # "logicalused",
    # "referenced",
    # "used",
    # "usedbydataset",
    # "usedbysnapshots",
    # "written",
)

ALL_POSSIBLE_PROPS = (
    "type",
    "creation",
    "used",
    "available",
    "referenced",
    "compressratio",
    "mounted",
    "quota",
    "reservation",
    "recordsize",
    "mountpoint",
    "sharenfs",
    "checksum",
    "compression",
    "atime",
    "devices",
    "exec",
    "setuid",
    "readonly",
    "zoned",
    "snapdir",
    "aclmode",
    "aclinherit",
    "createtxg",
    "canmount",
    "xattr",
    "copies",
    "version",
    "utf8only",
    "normalization",
    "casesensitivity",
    "vscan",
    "nbmand",
    "sharesmb",
    "refquota",
    "refreservation",
    "guid",
    "primarycache",
    "secondarycache",
    "usedbysnapshots",
    "usedbydataset",
    "usedbychildren",
    "usedbyrefreservation",
    "logbias",
    "dedup",
    "mlslabel",
    "sync",
    "dnodesize",
    "refcompressratio",
    "written",
    "logicalused",
    "logicalreferenced",
    "filesystem_limit",
    "snapshot_limit",
    "filesystem_count",
    "snapshot_count",
    "redundant_metadata",
    "special_small_blocks",
    "encryption",
    "keylocation",
    "keyformat",
    "pbkdf2iters",
    "smartfolders",
    "smartfs",
    "racktop:ub",
    "racktop:storage_profile",
    "racktop:ub_thresholds",
    "racktop:ub_suspend",
    "racktop:ub_trial",
    "racktop:version",
    "racktop:encoded_description",
)


class Dataset:
    """
    Dataset is class encapsulating methods used to manage datasets on the
    BrickStor appliance.
    """

    # Most of these settings we will never change. But, we are passing all the
    # settings to the backend as opposed to inheriting settings from the parent
    # dataset when we are creating a new dataset. Otherwise the dataset we are
    # creating may inherit settings which we do not actually desire.
    DEFAULT_DATASET_PROPS = {
        "aclinherit": "passthrough",
        "aclmode": "passthrough",
        "atime": "on",
        "canmount": "on",
        "checksum": "fletcher4",
        "compression": "lz4",
        "copies": 1,
        "devices": "on",
        "exec": "on",
        "filesystem_limit": None,
        "logbias": "latency",
        "nbmand": "on",
        "casesensitivity": "mixed",
        "normalization": None,
        "utf8only": "off",
        "primarycache": "all",
        "quota": None,
        "readonly": "off",
        "recordsize": 131072,
        "redundant_metadata": "all",
        "refquota": None,
        "refreservation": None,
        "reservation": None,
        "secondarycache": "all",
        "setuid": "on",
        "snapdir": "hidden",
        "snapshot_limit": None,
        "sync": "standard",
        "vscan": "off",
        "xattr": "on",
        "zoned": "off",
        "racktop:storage_profile": "general_filesystem",
        "racktop:encoded_description": "",
        "smartfolders": "off",
        "racktop:ub": "on",
        "racktop:ub_suspend": "",
        "racktop:ub_thresholds": "null",  # We store some things as 'none' and some as 'null', like this one
        "racktop:ub_trial": "",
        "racktop:version": 1,
        "sharenfs": "off",
        "sharesmb": "off",
    }
    """Describes dataset properties. The API returns a JSON representation
    which we convert into a Python native object. This same class is also used
    to realize changes to existing datasets or creation of new datasets with
    desired property settings.
    """

    def __init__(self, props=None) -> None:
        self.props = None
        # If props is not None or empty, we deepcopy the contents into our own
        # self.props attribute.
        if props is not None:
            self.props = deepcopy(props)

    def merge(self, **changes):
        """Merges dataset properties that are part of this class with supplied properties.

        Raises:
            KeyError: A key must be known in order for update to succeed. Unknown keys cause an exception to be raised.
        """
        for k, v in changes.items():
            if not k in KNOWN_PROPS:
                raise KeyError(
                    f"Cannot update '{k}'; because it is not a known property name"
                )
            # We do a bunch of hacky stuff here when we merge settings, because
            # there are inconsistencies in the representation of the data
            # between ZFS props, the API and defaults here.
            if type(v) != type(self.props[k]):
                if isinstance(self.props[k], int):
                    if v is None:
                        self.props[k] = None
                        continue
            self.props[k] = v

    def diff(self, other: Dict):
        # First we create a temporary _partial_ view of our properties,
        # containing only props present in the 'other' dict. We then return
        # what amounts to a set difference between our dict and 'other'.
        partial = dict()
        # If 'other' is an empty dict, we are going to get back an empty dict
        # since there won't be anything selected from our dict.
        for k, _ in other.items():
            if k not in self.props:
                raise KeyError(f"Property '{k}' is not known")
            partial[k] = self.props[k]
        return dict(set(other.items()) - set(partial.items()))

    @property
    def _dataset_props(self) -> Dict:
        """Tweaks certain dataset properties to make sure that they are accepted by the API.

        Returns:
            Dict: Properties formatted correctly for comsumption by the API.
        """
        output = dict()
        for k, v in self.props.items():
            if v == None:
                output[k] = "none"
            elif isinstance(v, int):
                output[k] = str(v)
            else:
                output[k] = v
        return output

    def _filtered_dict(self, d: Dict, excludes: List[str]):
        """Filters a given dict by excluding keys matching those in the excludes list.

        Args:
            d (Dict): Dictionary to filter with the excludes list.
            excludes (List[str]): List of keys to exclude from the dict.

        Yields:
            Tuple[str, Any]: Key/Value pairs that made it through the filter.
        """
        for k, v in d.items():
            if k in excludes:
                continue
            yield (k, v)

    def _seed_with_default_props(self):
        """
        Initializes internal dataset representation with default dataset properties.
        """
        if self.props is None:
            self.props = deepcopy(Dataset.DEFAULT_DATASET_PROPS)
        else:
            self.props.update(Dataset.DEFAULT_DATASET_PROPS)

    def set_dataset_properties(
        self, ds_path, api_client: AnsibleBsrApiClient, **props
    ) -> DatasetTaskResult:
        """Updates a dataset with supplied props via the shell API.

        Args:
            ds_path (str): Path to the dataset to be created.
            api_client (AnsibleBsrApiClient): Configured API client object.

        Returns:
            DatasetTaskResult: Describes the outcome from the request made to the shell API.
        """
        # First, we need to determine if changes are necessary.
        current_properties = dict()
        try:
            current_properties = api_client.get_dataset_properties(ds_path)
        except DatasetQueryError as err:
            if err.error_code == DatasetErrors.DoesNotExist:
                return DatasetTaskResult(
                    succeeded=False,
                    changed=False,
                    details={
                        "dataset_absent": True,
                    },
                    error="Unable to obtain current properties because dataset does not exist",
                )
        partial = dict()
        for key in props.keys():
            partial[key] = current_properties[key]
        changes = dict(set(props.items() - set(partial.items())))
        if not changes:
            return DatasetTaskResult(
                succeeded=True,
                changed=False,
                details={},
                error=None,
            )
        _ = api_client.set_dataset_properties(ds_path, **changes)
        return DatasetTaskResult(
            succeeded=True,
            changed=True,
            error=None,
            details=changes,
        )

    def create_dataset(
        self, ds_path, api_client: AnsibleBsrApiClient, **props
    ) -> DatasetTaskResult:
        """Creates a dataset via the shell API.

        Args:
            ds_path (str): Path to the dataset to be created.
            api_client (AnsibleBsrApiClient): Configured API client object.

        Returns:
            DatasetTaskResult: Describes the outcome from the request made to the shell API.
        """
        # If we are setting specific properties, merge them into the pre-defined
        # properties dict.
        create_err = None
        try:
            self._seed_with_default_props()
            if props:
                self.merge(**props)
        except KeyError as err:
            return DatasetTaskResult(
                succeeded=False,
                changed=False,
                error=err.args[0],
                details={},
            )
        except TypeError as err:
            return DatasetTaskResult(
                succeeded=False,
                changed=False,
                error=err.args[0],
                details={},
            )

        try:
            _ = api_client.create_dataset(ds_path, **self._dataset_props)
            return DatasetTaskResult(
                succeeded=True,
                changed=True,
                error=None,
                details={
                    "outcome": "created",
                },
            )
        except DatasetQueryError as err:
            # FIXME: This is a bit crap, but avoids having another try/except
            # nested under this top-level try/except.
            create_err = err
        # If the dataset already exists, we are going to apply properties
        # and see whether any changes were actually made by comparing
        # properties before and after we apply them.
        # Dataset create case: existing dataset
        if create_err.error_code == DatasetErrors.Exists:
            props_before = self._filtered_dict(
                api_client.get_dataset_properties(ds_path), IGNORED_PROPS
            )
            self.props = props
            # This is spaghetti code and it will need to be improved.
            # Exceptions here make sense, but nesting them is less than ideal.
            try:
                api_client.set_dataset_properties(ds_path, **self._dataset_props)
                props_after = self._filtered_dict(
                    api_client.get_dataset_properties(ds_path), IGNORED_PROPS
                )
                diff = dict(
                    set(set(dict(props_after).items() - dict(props_before).items()))
                )
                if diff:
                    return DatasetTaskResult(
                        succeeded=True,
                        changed=True,
                        error=None,
                        details={"outcome": "modified", "updates": diff},
                    )
                return DatasetTaskResult(
                    succeeded=True,
                    changed=False,
                    error=None,
                    details={
                        "outcome": "unchanged",
                    },
                )
            except DatasetQueryError as err:
                return DatasetTaskResult(
                    succeeded=False,
                    changed=False,
                    error=err.args[0],
                    details={},
                )
        # Dataset create case: space issue, could be quotas/reservations
        elif create_err.error_code == DatasetErrors.DoesNotHaveEnoughSpace:
            return DatasetTaskResult(
                succeeded=False,
                changed=False,
                error=create_err.args[0],
                details={},
            )
        # Dataset create case: issues other than existing or capacity
        else:
            return DatasetTaskResult(
                succeeded=False,
                changed=False,
                error=create_err.args[0],
                details={},
            )

    def destroy_dataset(
        self, ds_path, api_client: AnsibleBsrApiClient, recursive=False
    ):
        if not api_client.is_existing_dataset(ds_path):
            return DatasetTaskResult(
                succeeded=True,
                changed=False,
                error=None,
                details={"dataset_absent": True},
            )
        try:
            _ = api_client.destroy_dataset(ds_path, recursive)
        except DatasetQueryError as err:
            if err.error_code == DatasetErrors.RequiresRecursiveDestroy:
                err_msg, descendants = err.args
                return DatasetTaskResult(
                    succeeded=False,
                    changed=False,
                    error=err_msg,
                    details={
                        "descendants": descendants,
                    },
                )
            return DatasetTaskResult(
                succeeded=False, changed=False, error=err.args[0], details={}
            )
        return DatasetTaskResult(succeeded=True, changed=True, error=None, details={})

    def set_permissions(
        self,
        ds_path: str,
        acl: List[Dict[str, str]],
        owner_sid: str,
        owner_group_sid: str,
        api_client: AnsibleBsrApiClient,
        recursive=False,
    ):
        # Store previous settings and capture the dataset ID required in the
        # call to apply ACLs.
        try:
            old_settings = api_client.get_dataset_perms(ds_path)
            ds_id = old_settings.dataset_id
        except DatasetQueryError as err:
            return DatasetTaskResult(
                succeeded=False,
                changed=False,
                error=err.args[0],
                details={"operation": "lookup ACLs before making changes"},
            )
        try:
            new_settings = api_client.set_dataset_perms(
                ds_id, acl, owner_sid, owner_group_sid, recursive
            )
        except DatasetQueryError as err:
            return DatasetTaskResult(
                succeeded=False,
                changed=False,
                error=err.args[0],
                details={"operation": "application of ACLs"},
            )

        # We assume that the ACL array sort is stable. We want to figure out if
        # there was a state change. Therefore we compare elements which may
        # have changed.

        old_acl = old_settings.acl
        new_acl = new_settings.acl
        removed = []
        added = []
        owner_sid_changed = old_settings.owner_sid != new_settings.owner_sid
        owner_group_sid_changed = (
            old_settings.owner_group_sid != new_settings.owner_group_sid
        )

        # We want to set this to true if something changed.
        changed = owner_sid_changed or owner_group_sid_changed

        # Resolve differences between the original ACL and the new ACL.
        # We create two lists here, one which contains additions and another containing removals.
        for a in old_acl:
            found = False
            for b in new_acl:
                if a == b:
                    found = True
            if not found:
                removed.append(a)
        for a in new_acl:
            found = False
            for b in old_acl:
                if a == b:
                    found = True
            if not found:
                added.append(a)

        changed = changed or added != [] or removed != []

        return DatasetTaskResult(
            succeeded=True,
            changed=changed,
            error="",
            details={
                "added_acl": added,
                "removed_acl": removed,
                "owner_sid changed": owner_sid_changed,
                "owner_group_sid changed": owner_group_sid_changed,
            },
        )

    def configure_smb_share(
        self, ds_path: str, api_client: AnsibleBsrApiClient
    ) -> DatasetTaskResult:
        """Shares out a given dataset specified by ds_path with supplied share configuration.

        Args:
            ds_path (str): Path to dataset to be shared out.
            api_client (AnsibleBsrApiClient): Configured API client object.

        Returns:
            DatasetTaskResult: Describes the outcome from the request made to the shell API.
        """
        if not api_client.is_existing_dataset(ds_path):
            return DatasetTaskResult(
                succeeded=False,
                changed=False,
                error=f"cannot share non-existent dataset {ds_path}",
                details={},
            )

        filtered = tuple(
            [p for p in ALL_POSSIBLE_PROPS if p not in ("sharesmb", "racktop:ub")]
        )
        props_before = dict(
            self._filtered_dict(api_client.get_dataset_properties(ds_path), filtered)
        )

        try:
            api_client.set_dataset_properties(ds_path, **self._dataset_props)
        except DatasetQueryError as err:
            return DatasetTaskResult(
                succeeded=False,
                changed=False,
                error=err.args[0],
                details={"operation": "setting dataset share properties"},
            )

        props_after = dict(
            self._filtered_dict(api_client.get_dataset_properties(ds_path), filtered)
        )

        changed = dict(set(set(props_after.items() - props_before.items())))

        details = dict()
        if changed:
            details = (
                {
                    "before": dict(props_before),
                    "after": dict(props_after),
                    "changes": bool(changed),
                },
            )
        return DatasetTaskResult(
            succeeded=True,
            changed=bool(changed),
            error="",
            details=details,
        )
