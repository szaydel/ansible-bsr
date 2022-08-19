from dataclasses import dataclass
import unittest

from .netacl import IPNetHostList
from .netacl import raise_on_conflict


# 11 39:18 0.027879 [POST-4] 10.2.22.87> /usr/sbin/zfs set sharesmb=name=a,abe=true,csc=disabled,encrypt=required,ro=@10.255.4.0/24:@10.255.7.0/24,rw=@10.255.2.0/24:@10.255.5.0/24,none=@10.255.2.3:@10.255.2.4 p01/global/ahttps://10.2.22.87:8443/internal/v1/shell/run{"Command":"/usr/sbin/zfs","Args":"set sharesmb=name=a,abe=true,csc=disabled,encrypt=required,ro=@10.255.4.0/24:@10.255.7.0/24,rw=@10.255.2.0/24:@10.255.5.0/24,none=@10.255.2.3:@10.255.2.4 p01/global/a","UseShell":false,"IsQuery":false,"ActionId":"22ff7e60-91ec-4445-ab03-7ad519d6e382"}


@dataclass
class SMBShare:
    name: str
    abe: bool  # Access Based Enumeration
    csc: str  # Client-side Caching (enabling this breaks UB)
    encrypt: str
    read_only: str
    read_write: str
    none: str
    ub: bool

    @property
    def property_pairs(self):
        parts = []
        parts.append(f"name={self.name}")
        parts.append(f"abe={'true' if self.abe_setting else 'false'}")
        parts.append(f"csc={self.csc_setting}")
        parts.append(f"encrypt={self.encrypt_setting}")
        if self.read_only_list() is not None:
            parts.append(f"ro={self.fmt_read_only_list}")
        if self.read_write_list() is not None:
            parts.append(f"rw={self.fmt_read_write_list}")
        if self.none_list() is not None:
            parts.append(f"none={self.fmt_none_list}")
        return {
            "sharesmb": ",".join(parts),
            "racktop:ub": f"{'on' if self.ub_setting else 'off'}",
        }

    @property
    def csc_setting(self):
        legal_values = ("manual", "auto", "vdo", "disabled")
        if self.csc in legal_values:
            return self.csc
        raise ValueError(f"Invalid value for csc option: {self.csc}")

    @property
    def encrypt_setting(self):
        legal_values = ("enabled", "disabled", "required")
        if self.encrypt in legal_values:
            return self.encrypt

        raise ValueError(f"Invalid value for encrypt option: {self.encrypt}")

    @property
    def abe_setting(self):
        if isinstance(self.abe, bool):
            return self.abe
        raise TypeError(f"Invalid type for abe option: {self.abe}")

    @property
    def ub_setting(self):
        if isinstance(self.ub, bool):
            return self.ub
        raise TypeError(f"Invalid type for racktop:ub option: {self.abe}")

    def __str__(self):
        parts = []
        parts.append(f"name={self.name}")
        parts.append(f"abe={'true' if self.abe_setting else 'false'}")
        parts.append(f"csc={self.csc_setting}")
        parts.append(f"encrypt={self.encrypt_setting}")
        if self.read_only_list() is not None:
            parts.append(f"ro={self.fmt_read_only_list}")
        if self.read_write_list() is not None:
            parts.append(f"rw={self.fmt_read_write_list}")
        if self.none_list() is not None:
            parts.append(f"none={self.fmt_none_list}")
        return (
            "sharesmb="
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

    def read_write_list(self):
        """Coverts a given string to an read/write IPNetHostList.

        Returns:
            IPNetHostList: Read/write list represented as native objects.
        """
        if self.read_write:
            return IPNetHostList(self.read_write)

    def none_list(self):
        """Coverts a given string to an none IPNetHostList.

        Returns:
            IPNetHostList: None list represented as native objects.
        """
        if self.none:
            return IPNetHostList(self.none)


class TestSMBShare(unittest.TestCase):
    def test_smb_share_setting_is_correct(self):

        test_cases = (
            {
                "params": SMBShare(
                    name="test",
                    abe=True,
                    csc="disabled",
                    encrypt="required",
                    read_only="10.0.0.0/8:1.2.3.4:10.1.0.0/16:12.13.14.0/24:foobar.alpha.com",
                    read_write="12.13.15.3:5.6.0.0/24",
                    none="",
                    ub=True,
                ),
                "want": "sharesmb=name=test,abe=true,csc=disabled,encrypt=required,ro=@10.0.0.0/8:@10.1.0.0/16:@12.13.14.0/24:@1.2.3.4:foobar.alpha.com,rw=@5.6.0.0/24:@12.13.15.3 racktop:ub=on",
            },
            {
                "params": SMBShare(
                    name="test",
                    abe=True,
                    csc="disabled",
                    encrypt="required",
                    read_only="10.0.0.0/8:1.2.3.4:10.1.0.0/16:12.13.14.0/24:foobar.alpha.com",
                    read_write="12.13.15.3:5.6.0.0/24",
                    none="10.0.0.0/8",
                    ub=True,
                ),
                "want": "sharesmb=name=test,abe=true,csc=disabled,encrypt=required,ro=@10.0.0.0/8:@10.1.0.0/16:@12.13.14.0/24:@1.2.3.4:foobar.alpha.com,rw=@5.6.0.0/24:@12.13.15.3,none=@10.0.0.0/8 racktop:ub=on",
            },
            {
                "params": SMBShare(
                    name="test",
                    abe=True,
                    csc="disabled",
                    encrypt="required",
                    read_only="10.0.0.0/8:1.2.3.4:10.1.0.0/16:12.13.14.0/24:foobar.alpha.com",
                    read_write="12.13.15.3:5.6.0.0/24",
                    none="10.0.0.0/8:alpha.beta.com",
                    ub=False,
                ),
                "want": "sharesmb=name=test,abe=true,csc=disabled,encrypt=required,ro=@10.0.0.0/8:@10.1.0.0/16:@12.13.14.0/24:@1.2.3.4:foobar.alpha.com,rw=@5.6.0.0/24:@12.13.15.3,none=@10.0.0.0/8:alpha.beta.com racktop:ub=off",
            },
        )
        for case in test_cases:
            case["params"].validate_access_lists()
            actual = str(case["params"])
            self.assertEqual(actual, case["want"])

    def test_smb_share_invalid_settings(self):
        test_cases = (
            {
                "params": SMBShare(
                    name="test",
                    abe=True,
                    csc="disabled",
                    encrypt="bogus",
                    read_only="10.0.0.0/8:1.2.3.4:10.1.0.0/16:12.13.14.0/24:foobar.alpha.com",
                    read_write="12.13.15.3:5.6.0.0/24",
                    none="",
                    ub=True,
                ),
                "exception": ValueError,
            },
            {
                "params": SMBShare(
                    name="test",
                    abe=True,
                    csc="bogus",
                    encrypt="required",
                    read_only="10.0.0.0/8:1.2.3.4:10.1.0.0/16:12.13.14.0/24:foobar.alpha.com",
                    read_write="12.13.15.3:5.6.0.0/24",
                    none="10.0.0.0/8",
                    ub=True,
                ),
                "exception": ValueError,
            },
            {
                "params": SMBShare(
                    name="test",
                    abe=None,
                    csc="disabled",
                    encrypt="required",
                    read_only="10.0.0.0/8:1.2.3.4:10.1.0.0/16:12.13.14.0/24:foobar.alpha.com",
                    read_write="12.13.15.3:5.6.0.0/24",
                    none="10.0.0.0/8:alpha.beta.com",
                    ub=False,
                ),
                "exception": TypeError,
            },
        )
        for case in test_cases:
            with self.assertRaises(case["exception"]) as e:
                case["params"].validate_access_lists()
                _ = str(case["params"])
            self.assertRegex(
                e.exception.args[0], r"Invalid (type|value) for \S+ option: \S+"
            )
