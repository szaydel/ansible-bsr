from dataclasses import dataclass
import unittest

from .netacl import IPNetHostList
from .netacl import raise_on_conflict


@dataclass
class NFSShare:
    read_only: str
    read_write: str
    none: str
    root: str
    hideds: bool = False
    sec_label: bool = False
    ub: bool = True
    sec_mode: str = "sys"

    @property
    def security_label(self):
        if self.sec_label:
            return "seclabel"
        return None

    @property
    def security_mode(self):
        if not self.sec_mode:
            raise ValueError("Security mode cannot be empty")
        if not self.sec_mode in {
            "sys",
            "dh",
            "none",
            "krb5",
            "krb5i",
            "krb5p",
        }:
            raise ValueError(f"Invalid value for sec option: {self.sec_mode}")
        return self.sec_mode

    @property
    def ub_setting(self):
        if isinstance(self.ub, bool):
            return self.ub
        raise TypeError(f"Invalid type for racktop:ub option: {self.ub}")

    @property
    def hide_descendant_dataset(self):
        if isinstance(self.hideds, bool):
            if not self.hideds:
                return "nohide"
            else:
                return None
        raise TypeError(f"Invalid type for hideds option: {self.ub}")

    def __str__(self):
        parts = []
        parts.append(sec_label) if (sec_label := self.security_label) else None
        parts.append("anon=nobody")
        parts.append(f"sec={self.security_mode}")
        parts.append(hideds) if (hideds := self.hide_descendant_dataset) else None
        if self.read_only_list() is not None:
            parts.append(f"ro={self.fmt_read_only_list}")
        if self.read_write_list() is not None:
            parts.append(f"rw={self.fmt_read_write_list}")
        if self.none_list() is not None:
            parts.append(f"none={self.fmt_none_list}")
        if self.root_list() is not None:
            parts.append(f"root={self.fmt_root_list}")
        return (
            "sharenfs="
            + ",".join(parts)
            + " "
            + f"racktop:ub={'on' if self.ub_setting else 'off'}"
        )

    @property
    def fmt_read_write_list(self):
        """Formats read/write list and converts into a string usable with zfs set command.

        Returns:
            str: Read/write list as a string.
        """
        return str(self.read_write_list())

    @property
    def fmt_read_only_list(self):
        """Formats read-only list and converts into a string usable with zfs set command.

        Returns:
            str: Read-only list as a string.
        """
        return str(self.read_only_list())

    @property
    def fmt_none_list(self):
        """Formats other list and converts into a string usable with zfs set command.

        Returns:
            str: Other list as a string.
        """
        return str(self.none_list())

    @property
    def fmt_root_list(self):
        """Formats superuser list and converts into a string usable with zfs set command.

        Returns:
            str: Other list as a string.
        """
        return str(self.root_list())

    def validate_access_lists(self):
        """
        Validates that the configuration is sane and will be accepted. Triggers
        an exception if the configuration is not appropriate.
        """
        raise_on_conflict(self.read_only_list(), self.read_write_list())

    def read_only_list(self):
        """Coverts a given string to an read-only IPNetHostList.

        Returns:
            IPNetHostList: Read-only list represented as native objects.
        """
        if self.read_only:
            return IPNetHostList(self.read_only)
        return None

    def read_write_list(self):
        """Coverts a given string to an read/write IPNetHostList.

        Returns:
            IPNetHostList: Read/write list represented as native objects.
        """
        if self.read_write:
            return IPNetHostList(self.read_write)
        return None

    def none_list(self):
        """Coverts a given string to an none IPNetHostList.

        Returns:
            IPNetHostList: None list represented as native objects.
        """
        if self.none:
            return IPNetHostList(self.none)
        return None

    def root_list(self):
        """Coverts a given string to a superuser IPNetHostList.

        Returns:
            IPNetHostList: Superuser list represented as native objects.
        """
        if self.read_write:
            return IPNetHostList(self.root)
        return None


class TestNFSShare(unittest.TestCase):
    def test_nfs_share_setting_is_correct(self):

        test_cases = (
            {
                "params": NFSShare(
                    sec_label=True,
                    hideds=True,
                    read_only="10.0.0.0/8:1.2.3.4:10.1.0.0/16:12.13.14.0/24:foobar.alpha.com",
                    read_write="12.13.15.3:5.6.0.0/24",
                    none="",
                    root="5.6.7.8",
                    ub=True,
                ),
                "want": "sharenfs=seclabel,anon=nobody,sec=sys,ro=@10.0.0.0/8:@10.1.0.0/16:@12.13.14.0/24:@1.2.3.4:foobar.alpha.com,rw=@5.6.0.0/24:@12.13.15.3,root=@5.6.7.8 racktop:ub=on",
            },
            {
                "params": NFSShare(
                    sec_label=True,
                    hideds=False,
                    read_only="10.0.0.0/8:1.2.3.4:10.1.0.0/16:12.13.14.0/24:foobar.alpha.com",
                    read_write="12.13.15.3:5.6.0.0/24",
                    none="10.0.0.0/8",
                    root="5.6.7.8",
                    ub=True,
                ),
                "want": "sharenfs=seclabel,anon=nobody,sec=sys,nohide,ro=@10.0.0.0/8:@10.1.0.0/16:@12.13.14.0/24:@1.2.3.4:foobar.alpha.com,rw=@5.6.0.0/24:@12.13.15.3,none=@10.0.0.0/8,root=@5.6.7.8 racktop:ub=on",
            },
            {
                "params": NFSShare(
                    sec_label=False,
                    hideds=True,
                    read_only="10.0.0.0/8:1.2.3.4:10.1.0.0/16:12.13.14.0/24:foobar.alpha.com",
                    read_write="12.13.15.3:5.6.0.0/24",
                    none="10.0.0.0/8:alpha.beta.com",
                    root="5.6.7.8",
                    ub=False,
                ),
                "want": "sharenfs=anon=nobody,sec=sys,ro=@10.0.0.0/8:@10.1.0.0/16:@12.13.14.0/24:@1.2.3.4:foobar.alpha.com,rw=@5.6.0.0/24:@12.13.15.3,none=@10.0.0.0/8:alpha.beta.com,root=@5.6.7.8 racktop:ub=off",
            },
        )
        for case in test_cases:
            case["params"].validate_access_lists()
            actual = str(case["params"])
            self.assertEqual(actual, case["want"])

    def test_nfs_share_invalid_settings(self):
        test_cases = (
            {
                "params": NFSShare(
                    sec_label=True,
                    hideds="bogus",
                    read_only="10.0.0.0/8:1.2.3.4:10.1.0.0/16:12.13.14.0/24:foobar.alpha.com",
                    read_write="12.13.15.3:5.6.0.0/24",
                    none="",
                    root="255.256.257.258",
                    ub=True,
                ),
                "exception": TypeError,
            },
            {
                "params": NFSShare(
                    sec_label=True,
                    hideds=False,
                    read_only="10.0.0.0/8:1.2.3.4:10.1.0.0/16:12.13.14.0/24:foobar.alpha.com",
                    read_write="12.13.15.3:5.6.0.0/24",
                    none="10.0.0.0/8",
                    root="255.255.255.255",
                    sec_mode="bogus",
                    ub=True,
                ),
                "exception": ValueError,
            },
            {
                "params": NFSShare(
                    sec_label=False,
                    hideds=True,
                    sec_mode="bogus",
                    read_only="10.0.0.0/8:1.2.3.4:10.1.0.0/16:12.13.14.0/24:foobar.alpha.com",
                    read_write="12.13.15.3:5.6.0.0/24",
                    none="10.0.0.0/8:alpha.beta.com",
                    root="1.2.3",
                    ub=True,
                ),
                "exception": ValueError,
            },
        )
        for case in test_cases:
            print(case)
            with self.assertRaises(case["exception"]) as e:
                case["params"].validate_access_lists()
                _ = str(case["params"])
            self.assertRegex(
                e.exception.args[0], r"Invalid (type|value) for \S+ option: \S+"
            )
