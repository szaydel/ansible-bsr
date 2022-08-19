import ipaddress
import re
from typing import List, Tuple
import unittest


IP4_RE = re.compile(
    r"(\b25[0-5]|\b2[0-4][0-9]|\b[01]?[0-9][0-9]?)(\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)){3}"
)


class InvalidAddressSpecification(Exception):
    pass


class IPNetHostList:
    """Provides a class with convenience methods over the list of nets and hosts to be included in the ACL."""

    def __init__(self, addrs: str or List[str] = None):
        self._addrs_list = None
        if isinstance(addrs, list):
            self._addrs_list = addrs
        elif isinstance(addrs, str):
            self._addrs = addrs
            self._addrs_list = self._list_from_addrs_string()
        else:
            raise TypeError("Only str and list are valid types for addrs")
        self._hosts, self._nets, self._others = self._split_nets_and_hosts()

    @property
    def hosts(self):
        return self._hosts

    @property
    def nets(self):
        return self._nets

    @property
    def others(self):
        return self._others

    def __str__(self):
        addrs = []
        for elem in self.nets + self.hosts:
            addrs.append("@" + str(elem))
        addrs += self._others
        return ":".join(addrs)

    def __len__(self):
        return len(self.hosts) + len(self.nets)

    def _validate_addr(self, host_or_net_addr: str):
        # If there is a slash '/' in the address, it is a network.
        if "/" in host_or_net_addr:  # network address case
            try:
                # FIXME: We probably should be doing more validation here, but
                # for now this is sufficient to know that the thing is at least
                # valid.
                _ = ipaddress.IPv4Network(host_or_net_addr)
            except ipaddress.NetmaskValueError as err:
                raise InvalidAddressSpecification(err.args[0])
            except ValueError as err:
                raise InvalidAddressSpecification(err.args[0])
        else:  # host address case
            try:
                addr = ipaddress.IPv4Address(host_or_net_addr)
                # Loopback address is not legal in this context
                if addr.is_loopback:
                    raise InvalidAddressSpecification(
                        f"Local address {host_or_net_addr} not allowed"
                    )
            except ipaddress.AddressValueError as err:
                raise InvalidAddressSpecification(err.args[0])

    def _list_from_addrs_string(self) -> Tuple[str]:
        addrs_list = []
        tokens = self._addrs.split(":")
        if not tokens:
            return tuple()
        # Validate that what we have looks like valid IP addresses/ranges.
        for token in tokens:
            if token == "":
                continue
            # IP addresses and network ranges are prefixed with '@'.
            elif token[0] == "@":
                # If not valid, this will raise an exception.
                self._validate_addr(token[1:])
                addrs_list.append(token)
            # This is a IPv4 address, but apparently lacks the '@' prefix.
            elif IP4_RE.match(token):
                self._validate_addr(token)
                addrs_list.append("@" + token)
            # Anything else, i.e. hostname, FQDN, etc.
            else:
                addrs_list.append(token)
        return tuple(addrs_list)

    def _split_nets_and_hosts(
        self,
    ) -> Tuple[Tuple[ipaddress.IPv4Address], Tuple[ipaddress.IPv4Network], Tuple[str]]:
        addrs_list = self._addrs_list
        hosts: List[ipaddress.IPv4Address] = list()
        nets: List[ipaddress.IPv4Network] = list()
        other: List[str] = list()
        for elem in addrs_list:
            if elem == "":
                continue
            if elem[0].isalpha():
                other.append(elem)
            elif "/" in elem:
                if elem[0] == "@":
                    n = ipaddress.IPv4Network(elem[1:])
                else:
                    n = ipaddress.IPv4Network(elem)
                nets.append(n)
            else:
                if elem[0] == "@":
                    h = ipaddress.IPv4Address(elem[1:])
                else:
                    h = ipaddress.IPv4Address(elem)
                hosts.append(h)
        return tuple(hosts), tuple(nets), tuple(other)


def raise_on_conflict(ro_list: IPNetHostList, rw_list: IPNetHostList):
    """Raises an exception if there is a conflict between the two lists. These lists are mutually exclusive.

    Args:
        ro_list (IPNetHostList): List of networks and hosts in the Read-Only ACL
        rw_list (IPNetHostList): List of networks and hosts in the Read-Write ACL

    Raises:
        InvalidAddressSpecification: Any conflict, such as overlap between ACLs leads to this exception being raised.
    """
    ro_hosts = ro_list.hosts
    ro_nets = ro_list.nets
    ro_others = ro_list.others
    rw_hosts = rw_list.hosts
    rw_nets = rw_list.nets
    rw_others = rw_list.others
    # First, make sure we do not have any host addresses in both groups.
    for ro_host in ro_hosts:
        for rw_host in rw_hosts:
            if ro_host == rw_host:
                raise InvalidAddressSpecification(
                    f"Found host address {str(ro_host)} in read/write and read-only lists"
                )
    # Check network overlaps.
    for ro_net in ro_nets:
        for rw_net in rw_nets:
            if ro_net.overlaps(rw_net):
                raise InvalidAddressSpecification(
                    f"Found network address {str(ro_net)} in read/write and read-only lists"
                )
    # Check for any host addresses that may belong to network in the other
    # list.
    for ro_host in ro_hosts:
        for rw_net in rw_nets:
            if ro_host in rw_net:
                raise InvalidAddressSpecification(
                    f"Found host address {str(ro_host)} in read-only list belonging to network address {rw_net} in read/write list"
                )
    for rw_host in rw_hosts:
        for ro_net in ro_nets:
            if rw_host in ro_net:
                raise InvalidAddressSpecification(
                    f"Found host address {str(rw_host)} in read/write list belonging to network address {ro_net} in read-only list"
                )
    for ro_other in ro_others:
        if ro_other in rw_others:
            raise InvalidAddressSpecification(
                f"Found name {ro_other} in read-only and read/write lists"
            )


class TestIPNetHostList(unittest.TestCase):
    def test_list_from_addrs_string_is_correct(self):
        addrs = IPNetHostList(
            "10.100.10.0/24:10.2.0.0/16:192.168.100.1:192.168.100.5:alpha.beta.com:beta.gamma.epsilon"
        )
        self.assertEqual(
            addrs._addrs_list,
            (
                "@10.100.10.0/24",
                "@10.2.0.0/16",
                "@192.168.100.1",
                "@192.168.100.5",
                "alpha.beta.com",
                "beta.gamma.epsilon",
            ),
        )

    def test_list_creation_is_correct(self):
        test_cases = (
            {
                "list": "10.100.10.0/24:10.2.0.0/16:192.168.100.1:192.168.100.5:alpha.beta.gamma",
                "want": {
                    "hosts": (
                        ipaddress.IPv4Address("192.168.100.1"),
                        ipaddress.IPv4Address("192.168.100.5"),
                    ),
                    "nets": (
                        ipaddress.IPv4Network("10.100.10.0/24"),
                        ipaddress.IPv4Network("10.2.0.0/16"),
                    ),
                    "others": ("alpha.beta.gamma",),
                },
            },
            {
                "list": [
                    "10.100.10.0/24",
                    "10.2.0.0/16",
                    "192.168.100.1",
                    "192.168.100.5",
                    "alpha.beta.gamma",
                ],
                "want": {
                    "hosts": (
                        ipaddress.IPv4Address("192.168.100.1"),
                        ipaddress.IPv4Address("192.168.100.5"),
                    ),
                    "nets": (
                        ipaddress.IPv4Network("10.100.10.0/24"),
                        ipaddress.IPv4Network("10.2.0.0/16"),
                    ),
                    "others": ("alpha.beta.gamma",),
                },
            },
        )
        for case in test_cases:
            test_list = IPNetHostList(case["list"])
            self.assertEqual(test_list.hosts, case["want"]["hosts"])
            self.assertEqual(test_list.nets, case["want"]["nets"])
            self.assertEqual(test_list.others, case["want"]["others"])

    def test_net_has_host_bits(self):
        with self.assertRaises(InvalidAddressSpecification) as e:
            _ = IPNetHostList("10.1.2.0/16")
        self.assertEqual(
            e.exception.args[0],
            "10.1.2.0/16 has host bits set",
        )

    def test_validation_raises_exception(self):
        test_ro_list = IPNetHostList(
            "10.100.10.0/24:10.2.0.0/16:192.168.100.1:192.168.100.5"
        )
        test_rw_list = IPNetHostList(
            "10.100.10.0/24:10.2.0.0/16:192.168.100.1:192.168.100.5"
        )
        with self.assertRaises(InvalidAddressSpecification) as e:
            raise_on_conflict(test_ro_list, test_rw_list)
        self.assertEqual(
            e.exception.args[0],
            "Found host address 192.168.100.1 in read/write and read-only lists",
        )

        test_ro_list = IPNetHostList("10.100.10.5")
        test_rw_list = IPNetHostList("10.100.10.0/24")

        with self.assertRaises(InvalidAddressSpecification) as e:
            raise_on_conflict(test_ro_list, test_rw_list)
        self.assertEqual(
            e.exception.args[0],
            "Found host address 10.100.10.5 in read-only list belonging to network address 10.100.10.0/24 in read/write list",
        )

        test_rw_list = IPNetHostList("10.100.0.0/16")
        test_rw_list = IPNetHostList("10.100.10.5")

        with self.assertRaises(InvalidAddressSpecification) as e:
            raise_on_conflict(test_ro_list, test_rw_list)
        self.assertEqual(
            e.exception.args[0],
            "Found host address 10.100.10.5 in read/write and read-only lists",
        )

    def test_hosts_property_correct(self):
        test_list = IPNetHostList(
            "10.100.10.0/24:10.2.0.0/16:192.168.100.1:192.168.100.5"
        )
        self.assertEqual(
            test_list.hosts,
            (
                ipaddress.IPv4Address("192.168.100.1"),
                ipaddress.IPv4Address("192.168.100.5"),
            ),
        )

    def test_nets_property_correct(self):
        test_list = IPNetHostList(
            "10.100.10.0/24:10.2.0.0/16:192.168.100.1:192.168.100.5"
        )
        self.assertEqual(
            test_list.nets,
            (
                (
                    ipaddress.IPv4Network("10.100.10.0/24"),
                    ipaddress.IPv4Network("10.2.0.0/16"),
                )
            ),
        )

    def test_others_property_correct(self):
        test_list = IPNetHostList(
            "10.100.10.0/24:10.2.0.0/16:192.168.100.1:192.168.100.5:alpha.beta.com:beta.alpha.com"
        )
        self.assertEqual(
            test_list.others,
            ("alpha.beta.com", "beta.alpha.com"),
        )
