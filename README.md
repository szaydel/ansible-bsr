# ansible-bsr
We need to make sure the environment is setup correctly in order to detect modules and utility code. The following will setup the environment:
```
[(ba)sh]
export ANSIBLE_CONFIG=ansible/ansible.cfg

[fish]
set -gx ANSIBLE_CONFIG ansible/ansible.cfg
```
It is possible to have the password for the root user to come from the environment. Modules support the `ANSIBLE_BRICKSTOR_PASSWORD` environment variable as a fallback for the password argument, which is required. If omitted, the module is going to check the environment for existence of this variable, which will be used as the password if found. The following will setup the environment:
```
[(ba)sh]
export ANSIBLE_BRICKSTOR_PASSWORD=xxxx

[fish]
set -gx ANSIBLE_BRICKSTOR_PASSWORD xxxx
```

The assumption is that all operations are performed with the current working directory set to the location of this `README.md`.


Testing commands:
```
time ansible -v -i etc/ansible/hosts -e '{"dict": {"a": "1"}}' -u szaydel -m dataset -a "ds_path=p01/global/d password=$API_PASSWORD host=10.2.22.87 state=present atime=off quota=2M exp={{ dict }}" localhost
```