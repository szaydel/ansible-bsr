from copy import deepcopy
from dataclasses import dataclass
from typing import Dict, List

from .api import AnsibleBsrApiClient
from .api import DatasetErrors
from .api import DatasetQueryError
from .api import KNOWN_PROPS


@dataclass
class DatasetTaskResult:
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

# I will remove this junk once I am confident that it is no longer useful for
# troubleshooting purposes.
# {"Command":"/usr/racktop/sbin/bsrzfs","Args":"create -o aclinherit=passthrough -o aclmode=passthrough -o atime=on -o canmount=on -o checksum=fletcher4 -o compression=lz4 -o copies=1 -o devices=on -o exec=on -o filesystem_limit=none -o logbias=latency -o nbmand=on -o casesensitivity=mixed -o normalization=none -o utf8only=off -o primarycache=all -o quota=none -o readonly=off -o recordsize=131072 -o redundant_metadata=all
# -o refquota=none
# -o refreservation=none
# -o reservation=none
# -o secondarycache=all
# -o setuid=on
# -o snapdir=hidden
# -o snapshot_limit=none
# -o sync=standard
# -o vscan=off
# -o xattr=on
# -o zoned=off
# -o racktop:storage_profile=general_filesystem
# -o racktop:encoded_description=
# -o smartfolders=off
# -o racktop:ub_suspend=
# -o racktop:ub_thresholds=null
# -o racktop:ub_trial=
# -o racktop:version=1
# -o sharenfs=off
# -o sharesmb=off p01/global/xyz",
# "UseShell":false,"IsQuery":false,"ActionId":"647abe67-41ef-4019-809c-a607c9079154"}


# {"Operation":{"RequestTimestamp":"2022-07-26T21:50:44.771987259Z","ResponseTimestamp":"2022-07-26T21:50:44.795556469Z","TxId":"28336ad9f1af4da8b3ef6926bef1121d","ClientTxId":"","ApiVersion":"bsrapid/23.3.0DEV.0","IsComplete":true},"Result":{"ExitCode":1,"StdOut":"","StdErr":"cannot create \'bp/alpha\': dataset already exists","CmdString":"/usr/racktop/sbin/bsrzfs create -o aclinherit=passthrough -o aclmode=passthrough -o atime=on -o canmount=on -o checksum=fletcher4 -o compression=lz4 -o copies=1 -o devices=on -o exec=on -o filesystem_limit=none -o logbias=latency -o nbmand=on -o casesensitivity=mixed -o normalization=none -o utf8only=off -o primarycache=all -o quota=none -o readonly=off -o recordsize=131072 -o redundant_metadata=all -o refquota=none -o refreservation=none -o reservation=none -o secondarycache=all -o setuid=on -o snapdir=hidden -o snapshot_limit=none -o sync=standard -o vscan=off -o xattr=on -o zoned=off -o racktop:storage_profile=general_filesystem -o racktop:encoded_description= -o smartfolders=off -o racktop:ub_suspend= -o racktop:ub_thresholds=null -o racktop:ub_trial= -o racktop:version=1 -o sharenfs=off -o sharesmb=off bp/alpha","ExecutionTime":"23.231309ms"}


class Dataset:
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
        "racktop:ub_thresholds": None,
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
