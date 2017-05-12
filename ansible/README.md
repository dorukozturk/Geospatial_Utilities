# Ansible scripts
These scripts are used to launch a cluster of ec2 instances and provision it with the project to do large scale extraction transformation and loading on EC2.

To use this you must copy ```local_vars.example.yml``` to ```local_vars.yml``` and edit several of the variables there,  especially ```prefix``` and ```ansible_ssh_private_key_file```

You must also copy ```ec2.example.ini``` to ```ec2.ini```  and add an ```instance_filter``` that is equal to the ```prefix``` variable in your ```local_vars.yml```  This ensures that the dynamic inventory does not capture any other running ETL nodes

## Instances

Launch instances defined in ```local_vars.yml```
```sh
$> ansible-playbook -vv -e @local_vars.yml launch.yml
```

Terminate instance defined in ```local_vars.yml```
```sh
$> ansible-playbook -vv -e @local_vars.yml terminate.yml
```

## Provision

```sh
$> ansible-playbook -vv -e @local_vars.yml -i ec2.py provision.yml 
```

